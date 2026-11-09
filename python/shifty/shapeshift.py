"""Write Shapeshift (``.shm``) modules in Python.

Shapeshift is shifty's language for validation rules. This module builds
the same rules from Python values, and writes them as ``.shm`` text,
formatted as ``shifty shape fmt`` would, or raises them to SHACL::

    from shifty import shapeshift as ss
    from shifty.shapeshift import Class, Shape

    BRICK = ss.Namespace("brick", "https://brickschema.org/schema/Brick#")

    m = ss.Module("vav", base="urn:example/vav/")
    m.points = ss.Path(BRICK.hasPoint)

    @m.template
    def with_point(cls: Class, n: int = 1):
        "At least `n` points of class `cls`."
        return m.points.at_least(n, cls)

    m.vav = Shape(
        BRICK.VAV,                                  # is brick:VAV
        m.points.required(flow=BRICK.Air_Flow_Sensor),
        temp=with_point(BRICK.Zone_Air_Temperature_Sensor),
        for_every=BRICK.VAV,
    )

    print(m)             # the .shm text
    m.write("shapes/")   # vav.shm, and the modules it imports
    ttl = m.export()     # SHACL Turtle, for shifty.validate

Definitions
    Assigning to a module defines a name: ``m.zone_temperature = Shape(...)``
    is ``shape zone-temperature``, ``m.points = Path(...)`` is ``path points``.
    ``m["any-name"] = ...`` takes the name as written. Reading a name the
    module does not define yet refers to it, so shapes can be written in any
    order. ``@m.template`` and ``@m.path_template`` define templates from
    functions.

Shapes
    ``Shape(*lines, **labelled)`` is a conjunction of lines; keyword
    arguments are named lines (``fan: ...``). A class term alone is
    ``is C``, and a shape or template call alone composes it. ``a & b`` is
    the same as ``Shape(a, b)``, ``a | b`` is ``any of``, and ``~a`` is
    ``none of``. A shape no module names is written inline.

Paths
    A term is a property where a path goes, and combines with ``/``
    (sequence), ``|`` (alternative), ``~`` (inverse), and ``.plus``,
    ``.star``, ``.maybe`` (``+``, ``*``, ``?``). A path's methods make
    lines: ``p.required()``, ``p.each(T)``, ``p.at_least(2, T)``. Given
    keyword arguments, they make a group of named fields along the path:
    ``points.required(sat=BRICK.SAT, flow=BRICK.Air_Flow_Sensor)``.

Tests
    Where a test goes, a term is a class, a shape is a shape, and a Python
    value is that value.

Modules
    A module imports the modules whose definitions it uses, and declares the
    prefixes of the terms it uses. :meth:`Module.uses` imports under an
    alias, or imports RDF.
"""

from __future__ import annotations

import copy
import difflib
import inspect
import pathlib
import re
import tempfile
from dataclasses import dataclass
from typing import (
    Any,
    Callable,
    Dict,
    Iterable,
    Iterator,
    List,
    Optional,
    Sequence,
    Tuple,
    Union,
)

from . import _shifty

__all__ = [
    "SELF",
    "Class",
    "Diagnostic",
    "Line",
    "Module",
    "Namespace",
    "P",
    "Path",
    "Shape",
    "ShapeshiftError",
    "Term",
    "all_of",
    "any_of",
    "at_least_of",
    "datatype",
    "every",
    "exactly_one_of",
    "format",
    "iri",
    "is_",
    "language",
    "length",
    "lit",
    "matching",
    "node",
    "node_kind",
    "none_of",
    "objects_of",
    "one_of",
    "pattern",
    "row",
    "sparql",
    "subjects_of",
    "value_range",
    "when",
    "write",
]

ShapeshiftError = _shifty.ShapeshiftError
"""Raised when generated text does not parse or build; carries the diagnostics."""

_KEYWORDS = frozenset(_shifty._shapeshift_keywords())
_NAME = re.compile(r"^[A-Za-z_][A-Za-z0-9_-]*$")
_PN_LOCAL = re.compile(r"^[A-Za-z0-9_]([A-Za-z0-9_.%-]*[A-Za-z0-9_%-])?$")
_BUILTIN_DATATYPES = frozenset(
    [
        "string",
        "integer",
        "decimal",
        "double",
        "float",
        "boolean",
        "date",
        "dateTime",
        "time",
        "anyURI",
        "langString",
    ]
)
_SEVERITIES = ("info", "warning", "violation")


def _check_name(name: str, what: str) -> str:
    if not isinstance(name, str) or not _NAME.match(name):
        raise ValueError(
            f"{what} {name!r} is not a valid name: use letters, digits, `_` and `-`, "
            "starting with a letter or `_`"
        )
    if name in _KEYWORDS:
        raise ValueError(f"{what} {name!r} is a Shapeshift keyword")
    return name


def _kebab(name: str) -> str:
    """A Python identifier as a Shapeshift name: ``zone_temperature`` is ``zone-temperature``."""
    return name.replace("_", "-")


def _string(s: str) -> str:
    out = s.replace("\\", "\\\\").replace('"', '\\"')
    return (
        '"' + out.replace("\n", "\\n").replace("\t", "\\t").replace("\r", "\\r") + '"'
    )


def _comment_lines(comment: Optional[str]) -> List[str]:
    if comment is None:
        return []
    return ["-- " + c if c else "--" for c in inspect.cleandoc(comment).splitlines()]


def _annotations(
    doc: Optional[str], severity: Optional[str], messages: Sequence[str]
) -> List[str]:
    out = []
    if doc is not None:
        out.append("doc " + _string(doc))
    if severity is not None:
        out.append("severity " + severity)
    out += ["message " + _string(m) for m in messages]
    return out


def _check_severity(level: Optional[str]) -> Optional[str]:
    if level is not None and level not in _SEVERITIES:
        raise ValueError(f"{level!r} is not a severity: use one of {_SEVERITIES}")
    return level


# ----------------------------------------------------------------- rendering


class _Ctx:
    """What rendering one module needs: which prefixes and imports it uses."""

    def __init__(self, module: Module) -> None:
        self.module = module
        self.namespaces: Dict[str, Namespace] = {}
        self.imports: Dict[Module, Optional[str]] = {}

    def namespace(self, ns: Namespace) -> None:
        seen = self.namespaces.get(ns.prefix)
        if seen is not None and seen.iri != ns.iri:
            raise ValueError(
                f"prefix `{ns.prefix}:` stands for both <{seen.iri}> and <{ns.iri}> "
                f"in module {self.module.name!r}"
            )
        self.namespaces.setdefault(ns.prefix, ns)

    def ref(self, d: _Def) -> str:
        """The name of `d` as this module spells it."""
        owner, name = d._where()
        if owner is self.module:
            return name
        alias = self.module._aliases.get(owner)
        self.imports.setdefault(owner, alias)
        return f"{alias}.{name}" if alias else name


def _block(ctx: _Ctx, lines: Sequence[Line]) -> str:
    body = [text for line in lines for text in line._render(ctx)]
    if not body:
        return "{ }"
    return "{\n" + "\n".join(body) + "\n}"


def _lines(x: Any) -> List[Line]:
    """`x` as lines of a block."""
    if isinstance(x, (list, tuple)):
        return [line for item in x for line in _lines(item)]
    if isinstance(x, Line):
        return [x]
    if isinstance(x, (Shape, _Ref, Template, Call)):
        return [_Use(x)]
    if isinstance(x, Param):
        return [_Is(x)] if x.kind == "class" else [_Use(x)]
    if isinstance(x, Term):
        return [_Is(x)]
    if isinstance(x, _PathOps):
        raise TypeError(
            f"{x!r} is a path, not a line: say what it needs, as in `p.required()` "
            "or `p.each(T)`"
        )
    raise TypeError(
        f"{x!r} is not a line: use a class, a shape, a template call, "
        "or a path's methods (`p.required()`)"
    )


def _labelled(named: Dict[str, Any]) -> List[Line]:
    out: List[Line] = []
    for label, x in named.items():
        _check_name(label, "label")
        lines = _lines(x)
        if len(lines) != 1 or not lines[0]._labellable:
            raise TypeError(f"`{label}=` names a field or a composed shape, not {x!r}")
        out.append(lines[0]._replace(_label=label))
    return out


def _value(ctx: _Ctx, v: Any) -> str:
    if isinstance(v, Term):
        return v._render(ctx)
    if isinstance(v, Param):
        return v.name
    if isinstance(v, _Literal):
        return v._render()
    if isinstance(v, bool):
        return "true" if v else "false"
    if isinstance(v, (int, float)):
        return repr(v)
    if isinstance(v, str):
        return _string(v)
    raise TypeError(
        f"{v!r} is not a value: use a term, a string, a number, or `lit(...)`"
    )


def _is_literal(x: Any) -> bool:
    return isinstance(x, (str, int, float, bool, _Literal))


def _shape_ref(x: Any) -> bool:
    """Whether `x` is written as a shape's name (or call) where a test goes."""
    if isinstance(x, Shape):
        return x.name is not None
    return isinstance(x, (_Ref, Template, Call))


def _test(ctx: _Ctx, x: Any) -> str:
    """`x` where a test goes."""
    if isinstance(x, Term):
        return x._render(ctx)
    if _shape_ref(x):
        return _call_text(ctx, x)
    if isinstance(x, Shape):
        return _block(ctx, x._inline())
    if isinstance(x, Param):
        return x.name
    if isinstance(x, _Test):
        return x._render(ctx)
    if isinstance(x, (list, tuple, Line)):
        return _block(ctx, _lines(x))
    if _is_literal(x):
        return "is " + _value(ctx, x)
    raise TypeError(
        f"{x!r} is not a test: use a class, a shape, a value, or a test like `one_of(...)`"
    )


def _counted_test(ctx: _Ctx, x: Any) -> str:
    """The test after `at least n is`, where a value goes directly."""
    if _is_literal(x):
        return _value(ctx, x)
    if isinstance(x, _OneOf):
        return x._render(ctx)
    return _test(ctx, x)


def _int(n: Any) -> str:
    if isinstance(n, Param):
        return n.name
    if isinstance(n, bool) or not isinstance(n, int) or n < 0:
        raise TypeError(f"{n!r} is not a count: use a non-negative int or a parameter")
    return str(n)


def _arg(ctx: _Ctx, a: Any) -> str:
    """`a` as an argument to a template."""
    if isinstance(a, Param):
        return a.name
    if isinstance(a, Term):
        return a._render(ctx)
    if isinstance(a, _PathOps):
        return a._path(ctx)
    if isinstance(a, bool):
        return "true" if a else "false"
    if isinstance(a, int):
        return str(a)
    if isinstance(a, (str, float, _Literal)):
        return _value(ctx, a)
    if isinstance(a, _Test):
        return _block(ctx, [_Is(a)])
    if isinstance(a, (Shape, _Ref, Template, Call, Line, list, tuple)):
        return _test(ctx, a)
    raise TypeError(f"{a!r} cannot be passed to a template")


def _call_args(ctx: _Ctx, args: Tuple[Any, ...], kwargs: Dict[str, Any]) -> str:
    out = [_arg(ctx, a) for a in args]
    out += [f"{k} = {_arg(ctx, v)}" for k, v in kwargs.items()]
    return ", ".join(out)


def _call_text(ctx: _Ctx, x: Any) -> str:
    if isinstance(x, Call):
        args = _call_args(ctx, x.args, x.kwargs)
        name = ctx.ref(x.target)
        return f"{name}({args})" if args else name
    if isinstance(x, _Ref):
        x._resolve(Shape, Template)
    return ctx.ref(x)


# ------------------------------------------------------------- combinators


class _Logic:
    """``&``, ``|``, and ``~`` for anything that is a shape."""

    def __and__(self, other: Any) -> Shape:
        return Shape(self, other)

    def __rand__(self, other: Any) -> Shape:
        return Shape(other, self)

    def __or__(self, other: Any) -> Line:
        return _either(self, other)

    def __ror__(self, other: Any) -> Line:
        return _either(other, self)

    def __invert__(self) -> Line:
        return none_of(self)


def _either(a: Any, b: Any) -> Line:
    branches: List[List[Line]] = []
    for x in (a, b):
        if isinstance(x, _Frame) and x.phrase == "any of" and not x._annotated():
            branches += x.branches
        else:
            branches.append(_lines(x))
    return _Frame("any of", branches)


# --------------------------------------------------------------------- paths


class _PathOps:
    """Path operators, and the lines a path makes."""

    _prec = 3

    def _path(self, ctx: _Ctx) -> str:
        raise NotImplementedError

    def _operand(self, ctx: _Ctx, at_least: int) -> str:
        text = self._path(ctx)
        return f"({text})" if self._prec < at_least else text

    def __truediv__(self, other: Any) -> _Seq:
        return _Seq([self, _as_path(other)])

    def __rtruediv__(self, other: Any) -> _Seq:
        return _Seq([_as_path(other), self])

    def __or__(self, other: Any) -> Any:
        if not isinstance(other, _PathOps):
            return NotImplemented
        return _Alt([self, other])

    def __ror__(self, other: Any) -> Any:
        if not isinstance(other, _PathOps):
            return NotImplemented
        return _Alt([other, self])

    def __invert__(self) -> _PathOps:
        return _Inv(self)

    @property
    def plus(self) -> _PathOps:
        """One or more steps: ``p+``."""
        return _Mod(self, "+")

    @property
    def star(self) -> _PathOps:
        """Zero or more steps: ``p*``."""
        return _Mod(self, "*")

    @property
    def maybe(self) -> _PathOps:
        """Zero or one step: ``p?``."""
        return _Mod(self, "?")

    def required(self, *rows: Any, **named: Any) -> Line:
        """``p required``: at least one value. With rows, a group."""
        return _counted(self, _Count("required"), rows, named, single_is=False)

    def optional(self, *rows: Any, **named: Any) -> Line:
        """``p optional``: at most one value. With rows, a group."""
        return _counted(self, _Count("optional"), rows, named, single_is=False)

    def exactly(self, n: Any, *rows: Any, **named: Any) -> Line:
        """``p exactly n``; ``p exactly n is T`` given one test; with rows, a group."""
        return _counted(self, _Count("exactly", n), rows, named)

    def at_least(self, n: Any, *rows: Any, **named: Any) -> Line:
        """``p at least n``; ``p at least n is T`` given one test; with rows, a group."""
        return _counted(self, _Count("at least", n), rows, named)

    def at_most(self, n: Any, *rows: Any, **named: Any) -> Line:
        """``p at most n``; ``p at most n is T`` given one test; with rows, a group."""
        return _counted(self, _Count("at most", n), rows, named)

    def between(self, lo: Any, hi: Any, *rows: Any, **named: Any) -> Line:
        """``p between lo and hi``; with rows, a group."""
        return _counted(self, _Count("between", lo, hi), rows, named, single_is=False)

    def has(self, *rows: Any, **named: Any) -> Line:
        """``p { ... }``: named fields along `p`, each exactly once unless its row says."""
        if not rows and not named:
            raise TypeError("`has` takes rows: `points.has(sat=BRICK.SAT, ...)`")
        return Group(self, None, _rows(rows, named))

    def each(self, *tests: Any) -> Field:
        """``p each T``: every value passes each test."""
        return Field(self).each(*tests)

    def same_as(self, other: Any) -> Field:
        """``p same as q``."""
        return Field(self).same_as(other)

    def different_from(self, other: Any) -> Field:
        """``p different from q``."""
        return Field(self).different_from(other)

    def less_than(self, other: Any) -> Field:
        """``p less than q``."""
        return Field(self).less_than(other)

    def unique_language(self) -> Field:
        """``p unique language``."""
        return Field(self).unique_language()


def _as_path(x: Any) -> _PathOps:
    if isinstance(x, _PathOps):
        return x
    raise TypeError(f"{x!r} is not a path: use a term, a path, or a path expression")


class _Seq(_PathOps):
    _prec = 1

    def __init__(self, parts: List[_PathOps]) -> None:
        self.parts = [
            q for p in parts for q in (p.parts if isinstance(p, _Seq) else [p])
        ]

    def _path(self, ctx: _Ctx) -> str:
        return "/".join(p._operand(ctx, 2) for p in self.parts)


class _Alt(_PathOps):
    _prec = 0

    def __init__(self, alts: List[_PathOps]) -> None:
        self.alts = [q for p in alts for q in (p.alts if isinstance(p, _Alt) else [p])]

    def _path(self, ctx: _Ctx) -> str:
        return " or ".join(p._operand(ctx, 1) for p in self.alts)


class _Inv(_PathOps):
    _prec = 2

    def __init__(self, inner: _PathOps) -> None:
        self.inner = inner

    def _path(self, ctx: _Ctx) -> str:
        return "^" + self.inner._operand(ctx, 3)


class _Mod(_PathOps):
    _prec = 2

    def __init__(self, inner: _PathOps, op: str) -> None:
        self.inner = inner
        self.op = op

    def _path(self, ctx: _Ctx) -> str:
        return self.inner._operand(ctx, 3) + self.op


class _Self(_PathOps):
    def _path(self, ctx: _Ctx) -> str:
        return "self"

    def __repr__(self) -> str:
        return "SELF"


SELF: _PathOps = _Self()
"""The empty path: the focus node itself."""


class Namespace:
    """A prefix and the IRI it stands for; its attributes are its terms.

    ``BRICK.VAV`` and ``BRICK["Zone_Air_Temperature_Sensor"]`` are terms.
    A module that uses one declares the prefix.
    """

    def __init__(self, prefix: str, iri: str) -> None:
        if not re.match(r"^[A-Za-z][A-Za-z0-9_-]*$", prefix):
            raise ValueError(f"{prefix!r} is not a valid prefix")
        self.prefix = prefix
        self.iri = iri

    def __getattr__(self, local: str) -> Term:
        if local.startswith("_"):
            raise AttributeError(local)
        return Term(self.iri + local, self, local)

    def __getitem__(self, local: str) -> Term:
        return Term(self.iri + local, self, local)

    def __repr__(self) -> str:
        return f"Namespace({self.prefix!r}, {self.iri!r})"


class Term(_PathOps):
    """An IRI: a property where a path goes, a class where a test goes.

    ``C & lines`` is the instances of `C` that satisfy `lines`.
    """

    def __init__(
        self, iri: str, namespace: Optional[Namespace] = None, local: str = ""
    ) -> None:
        self.iri = iri
        self.namespace = namespace
        self.local = local

    def _render(self, ctx: _Ctx) -> str:
        if self.namespace is not None and _PN_LOCAL.match(self.local):
            ctx.namespace(self.namespace)
            return f"{self.namespace.prefix}:{self.local}"
        return f"<{self.iri}>"

    def _path(self, ctx: _Ctx) -> str:
        return self._render(ctx)

    def __and__(self, other: Any) -> Shape:
        return Shape(self, other)

    def __rand__(self, other: Any) -> Shape:
        return Shape(other, self)

    def __eq__(self, other: object) -> bool:
        return isinstance(other, Term) and other.iri == self.iri

    def __hash__(self) -> int:
        return hash(self.iri)

    def __repr__(self) -> str:
        if self.namespace is not None:
            return f"{self.namespace.prefix}:{self.local}"
        return f"<{self.iri}>"


Class = Term
"""Annotate a template parameter with ``Class`` to make it a class: alone on a
line, it is then ``is cls`` rather than a composed shape."""


def iri(value: str) -> Term:
    """A term written as a full IRI: ``iri("http://example.org/p")``."""
    return Term(value)


RDF = Namespace("rdf", "http://www.w3.org/1999/02/22-rdf-syntax-ns#")
RDFS = Namespace("rdfs", "http://www.w3.org/2000/01/rdf-schema#")
XSD = Namespace("xsd", "http://www.w3.org/2001/XMLSchema#")
OWL = Namespace("owl", "http://www.w3.org/2002/07/owl#")


@dataclass(frozen=True)
class _Literal:
    value: str
    lang: Optional[str] = None

    def _render(self) -> str:
        return _string(self.value) + (f"@{self.lang}" if self.lang else "")


def lit(value: str, lang: Optional[str] = None) -> Any:
    """A string literal with a language tag: ``lit("Pump", "en")``."""
    return _Literal(value, lang)


# --------------------------------------------------------------------- tests


class _Test:
    def _render(self, ctx: _Ctx) -> str:
        raise NotImplementedError


class _OneOf(_Test):
    def __init__(self, values: Tuple[Any, ...]) -> None:
        if not values:
            raise TypeError("`one_of` takes at least one value")
        self.values = values

    def _render(self, ctx: _Ctx) -> str:
        return "one of " + " ".join(_value(ctx, v) for v in self.values)


def one_of(*values: Any) -> Any:
    """The value is one of these nodes or literals: ``one of a b c``."""
    return _OneOf(values)


class _Word(_Test):
    def __init__(self, render: Callable[[_Ctx], str]) -> None:
        self._text = render

    def _render(self, ctx: _Ctx) -> str:
        return self._text(ctx)


def datatype(dt: Union[str, Term]) -> Any:
    """A literal of this datatype: a built-in name like ``"string"``, or a term."""
    if isinstance(dt, str):
        if dt not in _BUILTIN_DATATYPES:
            raise ValueError(
                f"{dt!r} is not a built-in datatype ({', '.join(sorted(_BUILTIN_DATATYPES))}); "
                "pass a term for any other"
            )
        return _Word(lambda ctx: dt)
    return _Word(lambda ctx: "datatype " + dt._render(ctx))


def node_kind(kind: str) -> Any:
    """``"iri"``, ``"literal"``, or ``"blank"``."""
    if kind not in ("iri", "literal", "blank"):
        raise ValueError(
            f"{kind!r} is not a node kind: use 'iri', 'literal', or 'blank'"
        )
    return _Word(lambda ctx: kind)


def length(
    n: Optional[int] = None, *, min: Optional[int] = None, max: Optional[int] = None
) -> Any:
    """A lexical length: ``length(3)``, ``length(min=1)``, ``length(min=1, max=8)``."""
    if n is not None:
        if min is not None or max is not None:
            raise TypeError("`length` takes an exact length or bounds, not both")
        return _Word(lambda ctx: f"length {_int(n)}")
    if min is not None and max is not None:
        return _Word(lambda ctx: f"length {_int(min)} to {_int(max)}")
    if min is not None:
        return _Word(lambda ctx: f"length at least {_int(min)}")
    if max is not None:
        return _Word(lambda ctx: f"length at most {_int(max)}")
    raise TypeError("`length` takes an exact length, a minimum, a maximum, or both")


def value_range(lo: Any = None, hi: Any = None) -> Any:
    """A numeric range, inclusive: ``value_range(0, 100)``, ``value_range(hi=1.5)``."""
    if lo is not None and hi is not None:
        return _Word(lambda ctx: f"from {lo!r} to {hi!r}")
    if lo is not None:
        return _Word(lambda ctx: f"from {lo!r}")
    if hi is not None:
        return _Word(lambda ctx: f"up to {hi!r}")
    raise TypeError("`value_range` takes a lower bound, an upper bound, or both")


def pattern(regex: str, flags: str = "") -> Any:
    """The lexical form matches `regex`."""
    text = f"pattern {_string(regex)}" + (f" flags {_string(flags)}" if flags else "")
    return _Word(lambda ctx: text)


def language(*tags: str) -> Any:
    """A string tagged with one of these languages."""
    if not tags:
        raise TypeError("`language` takes at least one tag")
    return _Word(lambda ctx: "language " + " ".join(_string(t) for t in tags))


# --------------------------------------------------------------------- lines


class Line(_Logic):
    """One line of a shape. The annotation methods return a copy."""

    _label: Optional[str] = None
    _doc: Optional[str] = None
    _severity: Optional[str] = None
    _messages: Tuple[str, ...] = ()
    _comment: Optional[str] = None
    _labellable = False

    def _replace(self, **fields: Any) -> Any:
        new = copy.copy(self)
        for k, v in fields.items():
            setattr(new, k, v)
        return new

    def _annotated(self) -> bool:
        return bool(
            self._label
            or self._doc
            or self._severity
            or self._messages
            or self._comment
        )

    def doc(self, text: str) -> Any:
        """Document the line (``rdfs:comment`` on export)."""
        return self._replace(_doc=text)

    def severity(self, level: str) -> Any:
        """``"info"``, ``"warning"``, or ``"violation"`` for failures of this line."""
        return self._replace(_severity=_check_severity(level))

    def message(self, text: str) -> Any:
        """A message for failures of this line."""
        return self._replace(_messages=(*self._messages, text))

    def comment(self, text: str) -> Any:
        """A ``--`` comment above the line."""
        return self._replace(_comment=text)

    def _render(self, ctx: _Ctx) -> List[str]:
        head = _comment_lines(self._comment)
        head += _annotations(self._doc, self._severity, self._messages)
        label = f"{self._label}: " if self._label else ""
        return head + [label + self._text(ctx)]

    def _text(self, ctx: _Ctx) -> str:
        raise NotImplementedError


class _Count:
    def __init__(self, kind: str, a: Any = None, b: Any = None) -> None:
        self.kind, self.a, self.b = kind, a, b

    def _render(self) -> str:
        if self.kind in ("required", "optional"):
            return self.kind
        if self.kind == "between":
            return f"between {_int(self.a)} and {_int(self.b)}"
        return f"{self.kind} {_int(self.a)}"


def _as_count(c: Any) -> Optional[_Count]:
    """A row count: ``"required"``, ``"optional"``, ``n``, or ``(min, max)``."""
    if c is None:
        return None
    if c in ("required", "optional"):
        return _Count(c)
    if isinstance(c, (int, Param)) and not isinstance(c, bool):
        return _Count("exactly", c)
    if isinstance(c, tuple) and len(c) == 2:
        lo, hi = c
        if lo is not None and hi is not None:
            return _Count("between", lo, hi)
        if lo is not None:
            return _Count("at least", lo)
        if hi is not None:
            return _Count("at most", hi)
    raise ValueError(
        f"{c!r} is not a count: use 'required', 'optional', n, or (min, max) with None for open"
    )


class Field(Line):
    """``path count, qualifiers``: what the values along a path must be."""

    _labellable = True

    def __init__(self, path: Any, count: Optional[_Count] = None) -> None:
        self.path = _as_path(path)
        self.count = count
        self.qualifiers: Tuple[Callable[[_Ctx], str], ...] = ()

    def _add(self, q: Callable[[_Ctx], str]) -> Field:
        return self._replace(qualifiers=(*self.qualifiers, q))

    def each(self, *tests: Any) -> Field:
        """Every value passes each test: ``each T``."""
        if not tests:
            raise TypeError("`each` takes at least one test")
        f = self
        for t in tests:
            f = f._add(lambda ctx, t=t: "each " + _test(ctx, t))
        return f

    def _qualified(self, word: str, n: Any, test: Any) -> Field:
        if test is None:
            if self.count is not None:
                raise TypeError(
                    "the field already has a count; give a test for a qualified count"
                )
            return self._replace(count=_Count(word, n))
        return self._add(lambda ctx: f"{word} {_int(n)} is {_counted_test(ctx, test)}")

    def exactly(self, n: Any, test: Any = None) -> Field:
        return self._qualified("exactly", n, test)

    def at_least(self, n: Any, test: Any = None) -> Field:
        return self._qualified("at least", n, test)

    def at_most(self, n: Any, test: Any = None) -> Field:
        return self._qualified("at most", n, test)

    def same_as(self, other: Any) -> Field:
        return self._add(lambda ctx: "same as " + _value(ctx, other))

    def different_from(self, other: Any) -> Field:
        return self._add(lambda ctx: "different from " + _value(ctx, other))

    def less_than(self, other: Any) -> Field:
        return self._add(lambda ctx: "less than " + _value(ctx, other))

    def unique_language(self) -> Field:
        return self._add(lambda ctx: "unique language")

    def _text(self, ctx: _Ctx) -> str:
        text = self.path._path(ctx)
        if self.count is not None:
            text += " " + self.count._render()
        quals = [q(ctx) for q in self.qualifiers]
        if quals:
            text += " " + ", ".join(quals)
        return text


class _Row:
    def __init__(
        self,
        item: Any,
        count: Optional[_Count] = None,
        doc: Optional[str] = None,
        label: Optional[str] = None,
    ) -> None:
        if isinstance(item, _Row):
            count = count or item.count
            doc = doc or item.doc
            item = item.item
        self.item, self.count, self.doc, self.label = item, count, doc, label

    def _render(self, ctx: _Ctx) -> str:
        item, body = _row_item(self.item)
        if isinstance(item, Term):
            text = item._render(ctx)
        elif isinstance(item, Param):
            text = item.name
        else:
            text = _call_text(ctx, item)
        if self.label:
            text = f"{self.label}: {text}"
        if self.count is not None:
            text += " " + self.count._render()
        if self.doc is not None:
            text += " doc " + _string(self.doc)
        if body:
            text += " " + _block(ctx, body)
        return text


def _row_item(x: Any) -> Tuple[Any, List[Line]]:
    """A row's item and the lines its values also satisfy: ``C & lines`` is ``C { lines }``."""
    if isinstance(x, (Term, Param)) or _shape_ref(x):
        return x, []
    if isinstance(x, Shape):
        lines = _flat(x._inline())
        first = lines[0] if lines else None
        if isinstance(first, _Is) and not first._annotated():
            if isinstance(first.test, (Term, Param)):
                return first.test, lines[1:]
        if isinstance(first, _Use) and not first._annotated():
            return first.target, lines[1:]
    raise TypeError(
        f"{x!r} cannot be a row: a row is a class or a shape, and `C & lines` "
        "adds lines its values satisfy"
    )


def row(item: Any, count: Any = None, *, doc: Optional[str] = None) -> Any:
    """A row of a group with its own count or doc.

    ``points.has(rat=row(BRICK.RAT, "optional", doc="if present"))``. The
    count is ``"required"``, ``"optional"``, ``n``, or ``(min, max)``.
    """
    return _Row(item, _as_count(count), doc)


def _rows(rows: Sequence[Any], named: Dict[str, Any]) -> List[_Row]:
    out = [_Row(r) for r in rows]
    out += [_Row(r, label=_check_name(k, "label")) for k, r in named.items()]
    return out


def _counted(
    path: _PathOps,
    count: _Count,
    rows: Sequence[Any],
    named: Dict[str, Any],
    single_is: bool = True,
) -> Line:
    if not rows and not named:
        return Field(path, count)
    if single_is and len(rows) == 1 and not named and not isinstance(rows[0], _Row):
        test = rows[0]
        return Field(path)._add(
            lambda ctx: f"{count.kind} {_int(count.a)} is {_counted_test(ctx, test)}"
        )
    return Group(path, count, _rows(rows, named))


class Group(Line):
    """``path count { label: item ... }``: named fields along one path."""

    def __init__(self, path: Any, count: Optional[_Count], rows: List[_Row]) -> None:
        self.path = _as_path(path)
        self.count = count
        self.rows = rows

    def _text(self, ctx: _Ctx) -> str:
        head = self.path._path(ctx)
        if self.count is not None:
            head += " " + self.count._render()
        return head + " {\n" + "\n".join(r._render(ctx) for r in self.rows) + "\n}"


class _Is(Line):
    def __init__(self, test: Any) -> None:
        self.test = test

    def _text(self, ctx: _Ctx) -> str:
        return "is " + _counted_test(ctx, self.test)


def is_(test: Any) -> Line:
    """``is T``, a test of the focus node: for a value, or a class parameter
    not annotated ``Class``. A class term alone on a line already means this."""
    return _Is(test)


class _Use(Line):
    """A shape on a line: composed by name, or written out if no module names it."""

    _labellable = True

    def __init__(self, target: Any) -> None:
        self.target = target

    def _render(self, ctx: _Ctx) -> List[str]:
        t = self.target
        if isinstance(t, Shape) and t.name is None:
            if self._annotated():
                raise ValueError(
                    "a shape no module names cannot be labelled or annotated on a line: "
                    "assign it to a module, or annotate its lines"
                )
            return [s for line in t._inline() for s in line._render(ctx)]
        return super()._render(ctx)

    def _text(self, ctx: _Ctx) -> str:
        if isinstance(self.target, Param):
            return self.target.name
        return _call_text(ctx, self.target)


def _flat(lines: List[Line]) -> List[Line]:
    """`lines`, with the lines of unnamed shapes in place."""
    out: List[Line] = []
    for line in lines:
        t = getattr(line, "target", None)
        if isinstance(line, _Use) and isinstance(t, Shape) and t.name is None:
            if not line._annotated():
                out += _flat(t._inline())
                continue
        out.append(line)
    return out


class _Frame(Line):
    def __init__(self, phrase: str, branches: List[List[Line]]) -> None:
        if not branches:
            raise TypeError(f"`{phrase}` takes at least one branch")
        self.phrase = phrase
        self.branches = branches

    def _text(self, ctx: _Ctx) -> str:
        return self.phrase + " " + " ".join(_block(ctx, b) for b in self.branches)


def _frame(phrase: str, branches: Sequence[Any], named: Dict[str, Any]) -> Line:
    out = [_lines(b) for b in branches] + [[line] for line in _labelled(named)]
    return _Frame(phrase, out)


def any_of(*branches: Any, **labelled: Any) -> Line:
    """At least one branch holds. A keyword argument is a branch of one named line.

    ``a | b`` is ``any_of(a, b)``.
    """
    return _frame("any of", branches, labelled)


def all_of(*branches: Any, **labelled: Any) -> Line:
    """Every branch holds."""
    return _frame("all of", branches, labelled)


def exactly_one_of(*branches: Any, **labelled: Any) -> Line:
    """Exactly one branch holds."""
    return _frame("exactly one of", branches, labelled)


def none_of(*branches: Any, **labelled: Any) -> Line:
    """No branch holds. ``~a`` is ``none_of(a)``."""
    return _frame("none of", branches, labelled)


def at_least_of(n: int, *branches: Any, **labelled: Any) -> Line:
    """At least `n` branches hold."""
    return _frame(f"at least {_int(n)} of", branches, labelled)


class _When(Line):
    def __init__(self, cond: List[Line], body: Optional[List[Line]] = None) -> None:
        self.cond = cond
        self.body = body

    def then(self, *lines: Any, **labelled: Any) -> Line:
        """The lines that must hold when the condition does."""
        return self._replace(body=_lines(list(lines)) + _labelled(labelled))

    def _text(self, ctx: _Ctx) -> str:
        if self.body is None:
            raise TypeError("`when(...)` needs `.then(...)`")
        return f"when {_block(ctx, self.cond)}\nthen {_block(ctx, self.body)}"


def when(*cond: Any) -> Any:
    """``when { cond } then { body }``: write ``when(cond).then(body)``."""
    return _When(_lines(list(cond)))


class _Closed(Line):
    def _text(self, ctx: _Ctx) -> str:
        return "only these properties"


# ------------------------------------------------------------------- targets


class _Target:
    def __init__(self, render: Callable[[_Ctx], str]) -> None:
        self._render = render


def every(cls: Term) -> Any:
    """``for every C``: instances of `cls`. ``Shape(for_every=C)`` says the same."""
    return _Target(lambda ctx: "for every " + cls._render(ctx))


def subjects_of(prop: Term) -> Any:
    """``for every node with p``."""
    return _Target(lambda ctx: "for every node with " + prop._render(ctx))


def objects_of(prop: Term) -> Any:
    """``for every value of p``."""
    return _Target(lambda ctx: "for every value of " + prop._render(ctx))


def node(value: Any) -> Any:
    """``for node X``."""
    return _Target(lambda ctx: "for node " + _value(ctx, value))


def matching(shape: Any) -> Any:
    """``for every node matching S``."""
    return _Target(lambda ctx: "for every node matching " + _call_text(ctx, shape))


def sparql(query: str) -> Any:
    """``for sparql "..."``: the nodes a SPARQL SELECT returns."""
    return _Target(lambda ctx: "for sparql " + _string(query))


# -------------------------------------------------------------- definitions


class _Def:
    """Something a module names."""

    module: Optional[Module] = None
    name: Optional[str] = None
    _doc: Optional[str] = None
    _comment: Optional[str] = None
    _severity: Optional[str] = None
    _messages: Tuple[str, ...] = ()

    def _where(self) -> Tuple[Module, str]:
        if self.module is None or self.name is None:
            raise ValueError(f"{self!r} is not defined in any module: assign it to one")
        return self.module, self.name

    def _attach(self, module: Module, name: str) -> None:
        self.module, self.name = module, name

    def _head(self) -> List[str]:
        return _comment_lines(self._comment) + _annotations(
            self._doc, self._severity, self._messages
        )

    def _render(self, ctx: _Ctx) -> List[str]:
        raise NotImplementedError

    def __repr__(self) -> str:
        if self.module is None:
            return f"<{type(self).__name__}>"
        return f"<{type(self).__name__} {self.module.name}.{self.name}>"


class Shape(_Logic, _Def):
    """A conjunction of lines; a named shape once a module names it.

    ``m.vav = Shape(BRICK.VAV, points.required(), fan=vfd(BRICK.Fan))``.
    Positional arguments are lines; keyword arguments are named lines.
    `for_every`, `targets`, `doc`, `comment`, `severity`, `message`, and
    `closed` (``only these properties``) belong to a named shape.
    """

    def __init__(
        self,
        *lines: Any,
        doc: Optional[str] = None,
        comment: Optional[str] = None,
        for_every: Union[Term, Sequence[Term], None] = None,
        targets: Iterable[Any] = (),
        severity: Optional[str] = None,
        message: Union[str, Sequence[str], None] = None,
        closed: bool = False,
        **labelled: Any,
    ) -> None:
        self.lines = _lines(list(lines)) + _labelled(labelled)
        self._doc = doc
        self._comment = comment
        ts = list(targets)
        if isinstance(for_every, Term):
            ts.insert(0, every(for_every))
        elif for_every is not None:
            ts[:0] = [every(c) for c in for_every]
        self.targets: List[_Target] = ts
        self._severity = _check_severity(severity)
        self._messages = (
            (message,) if isinstance(message, str) else tuple(message or ())
        )
        self.closed = closed

    def _body(self) -> List[Line]:
        return self.lines + ([_Closed()] if self.closed else [])

    def _inline(self) -> List[Line]:
        """The lines, for a shape written in place of a name."""
        if (
            self.targets
            or self._doc
            or self._comment
            or self._severity
            or self._messages
        ):
            raise ValueError(
                "only a named shape has targets, a doc, a comment, a severity, or "
                "messages: assign this one to a module (`m.name = Shape(...)`)"
            )
        return self._body()

    def _render(self, ctx: _Ctx) -> List[str]:
        head = f"shape {self.name}"
        if len(self.targets) == 1:
            head += " " + self.targets[0]._render(ctx)
        elif self.targets:
            head += "".join("\n" + t._render(ctx) for t in self.targets) + "\n"
        return [*self._head(), head + " " + _block(ctx, self._body())]


class Path(_PathOps, _Def):
    """``path name = expression``, once a module names it: ``m.points = Path(BRICK.hasPoint)``."""

    def __init__(
        self, expr: Any, *, doc: Optional[str] = None, comment: Optional[str] = None
    ):
        self.expr = _as_path(expr)
        self._doc = doc
        self._comment = comment

    def _path(self, ctx: _Ctx) -> str:
        if self.module is None:
            return self.expr._path(ctx)
        return ctx.ref(self)

    def _operand(self, ctx: _Ctx, at_least: int) -> str:
        if self.module is None:
            return self.expr._operand(ctx, at_least)
        return self._path(ctx)

    def _render(self, ctx: _Ctx) -> List[str]:
        return [*self._head(), f"path {self.name} = {self.expr._path(ctx)}"]


class Param(_PathOps):
    """A template parameter: a path, a test, a count, or an argument."""

    def __init__(self, name: str, kind: Optional[str] = None) -> None:
        self.name = name
        self.kind = kind

    def _path(self, ctx: _Ctx) -> str:
        return self.name

    def __and__(self, other: Any) -> Shape:
        return Shape(self, other)

    def __rand__(self, other: Any) -> Shape:
        return Shape(other, self)

    def __repr__(self) -> str:
        return f"P.{self.name}"


class _Params:
    """``P.cls`` is the parameter `cls`, for a default that uses an earlier one."""

    def __getattr__(self, name: str) -> Any:
        if name.startswith("_"):
            raise AttributeError(name)
        return Param(name)


P = _Params()


def _is_class(annotation: Any) -> bool:
    if annotation is Term:
        return True
    return isinstance(annotation, str) and annotation.rsplit(".", 1)[-1] in (
        "Class",
        "Term",
    )


class _Signature:
    """A template's parameters, from a Python function."""

    def __init__(self, owner: str, func: Callable[..., Any]) -> None:
        self.owner = owner
        self.func = func
        self.signature = inspect.signature(func)
        annotations = getattr(func, "__annotations__", {})
        self.params: List[Tuple[inspect.Parameter, Param]] = []
        for p in self.signature.parameters.values():
            if p.kind not in (p.POSITIONAL_OR_KEYWORD, p.KEYWORD_ONLY):
                raise TypeError(
                    f"template parameter `{p.name}` must be a plain parameter"
                )
            _check_name(p.name, "parameter")
            kind = "class" if _is_class(annotations.get(p.name)) else None
            self.params.append((p, Param(p.name, kind)))

    def bind(self, args: Tuple[Any, ...], kwargs: Dict[str, Any]) -> None:
        try:
            self.signature.bind(*args, **kwargs)
        except TypeError as e:
            raise TypeError(f"{self.owner}: {e}") from None

    def apply(self) -> Any:
        """The function's result, with each parameter standing in for itself."""
        args = [q for p, q in self.params if p.kind == p.POSITIONAL_OR_KEYWORD]
        kwargs = {p.name: q for p, q in self.params if p.kind == p.KEYWORD_ONLY}
        return self.func(*args, **kwargs)

    def text(self, ctx: _Ctx) -> str:
        parts = [
            p.name if p.default is p.empty else f"{p.name} = {_arg(ctx, p.default)}"
            for p, _ in self.params
        ]
        return f"({', '.join(parts)})" if parts else ""


def _docstring(func: Callable[..., Any]) -> Optional[str]:
    d = inspect.getdoc(func)
    return " ".join(d.split()) if d else None


class Template(_Def):
    """A shape with parameters, from a Python function; calling it makes a shape.

    The function is called once, when the module is written, with a
    stand-in for each parameter, and returns the template's lines.
    """

    def __init__(
        self,
        module: Module,
        name: str,
        func: Callable[..., Any],
        doc: Optional[str],
        comment: Optional[str],
    ) -> None:
        self.module, self.name = module, name
        self.sig = _Signature(name, func)
        self._doc = doc if doc is not None else _docstring(func)
        self._comment = comment

    def __call__(self, *args: Any, **kwargs: Any) -> Call:
        self.sig.bind(args, kwargs)
        return Call(self, args, kwargs)

    def _render(self, ctx: _Ctx) -> List[str]:
        head = f"shape {self.name}{self.sig.text(ctx)}"
        return [*self._head(), head + " " + _block(ctx, _lines(self.sig.apply()))]


class Call(_Logic):
    """A template applied to arguments: a shape to compose, test, or pass on."""

    def __init__(
        self, target: Template, args: Tuple[Any, ...], kwargs: Dict[str, Any]
    ) -> None:
        self.target = target
        self.args = args
        self.kwargs = kwargs

    def doc(self, text: str) -> Line:
        """This call on a line, documented."""
        return _Use(self).doc(text)

    def comment(self, text: str) -> Line:
        """This call on a line, with a comment above it."""
        return _Use(self).comment(text)

    def __repr__(self) -> str:
        return f"<Call {self.target.name}>"


class PathTemplate(_PathOps, _Def):
    """A path with parameters: ``downstream(BRICK.hasPoint)``."""

    def __init__(
        self,
        module: Module,
        name: str,
        func: Callable[..., Any],
        doc: Optional[str],
        comment: Optional[str],
    ) -> None:
        self.module, self.name = module, name
        self.sig = _Signature(name, func)
        self._doc = doc if doc is not None else _docstring(func)
        self._comment = comment

    def __call__(self, *args: Any, **kwargs: Any) -> _PathOps:
        self.sig.bind(args, kwargs)
        return _PathCall(self, args, kwargs)

    def _path(self, ctx: _Ctx) -> str:
        return ctx.ref(self)

    def _render(self, ctx: _Ctx) -> List[str]:
        expr = _as_path(self.sig.apply())
        return [
            *self._head(),
            f"path {self.name}{self.sig.text(ctx)} = {expr._path(ctx)}",
        ]


class _PathCall(_PathOps):
    def __init__(
        self, target: PathTemplate, args: Tuple[Any, ...], kwargs: Dict[str, Any]
    ) -> None:
        self.target = target
        self.args = args
        self.kwargs = kwargs

    def _path(self, ctx: _Ctx) -> str:
        args = _call_args(ctx, self.args, self.kwargs)
        name = ctx.ref(self.target)
        return f"{name}({args})" if args else name


class _Ref(_Logic, _Def):
    """A name a module does not define yet; it must by the time the module is written."""

    def __init__(self, module: Module, name: str) -> None:
        self.module, self.name = module, name

    def _resolve(self, *kinds: type) -> _Def:
        assert self.module is not None and self.name is not None
        d = self.module._names.get(self.name)
        if d is None:
            near = difflib.get_close_matches(self.name, list(self.module._names), n=1)
            hint = f"; did you mean {near[0]!r}?" if near else ""
            raise ValueError(
                f"module {self.module.name!r} defines no {self.name!r}{hint}"
            )
        if not isinstance(d, kinds):
            raise TypeError(
                f"{self.name!r} in module {self.module.name!r} is not a shape"
            )
        return d

    def __call__(self, *args: Any, **kwargs: Any) -> Any:
        raise TypeError(
            f"module {self.module.name if self.module else '?'!r} defines no template "
            f"{self.name!r} yet: define a template before calling it"
        )

    def __getattr__(self, attr: str) -> Any:
        if attr.startswith("_"):
            raise AttributeError(attr)
        raise AttributeError(
            f"{self.name!r} is not defined yet, so it has no `.{attr}`: "
            "define paths before using them"
        )


# ------------------------------------------------------------------ modules


@dataclass(frozen=True)
class Diagnostic:
    """An error or warning about a module, as ``shifty shape check`` reports it."""

    level: str
    message: str
    file: Optional[str]
    line: Optional[int]
    column: Optional[int]
    hint: Optional[str]
    rendered: str

    def __str__(self) -> str:
        return self.rendered


class Module:
    """A ``.shm`` file: shapes, templates, and paths, with a base IRI.

    Assign to define (``m.zone_temperature = Shape(...)``); read to use, even
    before the definition. Definitions keep the order they are made in. The
    module writes to ``<name>.shm`` and imports the modules whose
    definitions it uses.
    """

    def __init__(
        self, name: str, *, base: Optional[str] = None, comment: Optional[str] = None
    ) -> None:
        if not re.match(r"^[A-Za-z0-9_.-]+$", name):
            raise ValueError(f"{name!r} is not a valid module name (it names the file)")
        self._name = name
        self._base = base
        self._comment = comment
        self._defs: List[_Def] = []
        self._names: Dict[str, _Def] = {}
        self._aliases: Dict[Module, Optional[str]] = {}
        self._uses: List[Tuple[Union[Module, str], Optional[str]]] = []

    @property
    def name(self) -> str:
        """The module's name; it writes to ``<name>.shm``."""
        return self._name

    @property
    def base(self) -> Optional[str]:
        return self._base

    @property
    def filename(self) -> str:
        return f"{self._name}.shm"

    # definitions

    def __setattr__(self, attr: str, value: Any) -> None:
        if attr.startswith("_"):
            object.__setattr__(self, attr, value)
        elif hasattr(type(self), attr):
            raise AttributeError(
                f"`{attr}` is part of Module; define it as m[{_kebab(attr)!r}] = ..."
            )
        else:
            self._define(_kebab(attr), value)

    def __getattr__(self, attr: str) -> Any:
        if attr.startswith("_"):
            raise AttributeError(attr)
        return self[_kebab(attr)]

    def __setitem__(self, name: str, value: Any) -> None:
        self._define(name, value)

    def __getitem__(self, name: str) -> Any:
        """The definition `name`, or a reference to it if it is not defined yet."""
        d = self._names.get(name)
        return d if d is not None else _Ref(self, name)

    def __contains__(self, name: str) -> bool:
        return name in self._names

    def __iter__(self) -> Iterator[Any]:
        return iter(self._defs)

    def __dir__(self) -> List[str]:
        names = [
            n.replace("-", "_")
            for n in self._names
            if n.replace("-", "_").isidentifier()
        ]
        return sorted(set(super().__dir__()) | set(names))

    def _define(self, name: str, value: Any) -> Any:
        _check_name(name, "name")
        if name in self._names:
            raise ValueError(f"module {self._name!r} already defines {name!r}")
        if isinstance(value, (Template, PathTemplate)):
            if value.module is not self or value.name != name:
                raise ValueError(f"{value!r} is defined with `@{self._name}.template`")
            d: _Def = value
        elif isinstance(value, Shape) and value.module is None:
            d = value
        elif isinstance(value, Path) and value.module is None:
            d = value
        elif isinstance(value, Term):
            raise TypeError(
                f"{value!r} is a term: write `Path({value!r})` to name it as a path, "
                f"or `Shape({value!r})` for the shape of its instances"
            )
        elif isinstance(value, _PathOps) and not isinstance(value, Param):
            d = Path(value)
        else:
            d = Shape(value)
        d._attach(self, name)
        self._defs.append(d)
        self._names[name] = d
        return d

    def template(
        self,
        func: Optional[Callable[..., Any]] = None,
        *,
        name: Optional[str] = None,
        doc: Optional[str] = None,
        comment: Optional[str] = None,
    ) -> Any:
        """Decorate a function as a template; its parameters are the template's.

        The function returns the template's lines. Python defaults are the
        template's defaults, and ``P.cls`` refers to an earlier parameter;
        a parameter annotated ``Class`` is a class. The name is the
        function's with ``-`` for ``_``, unless given; the docstring is its doc.
        """

        def make(f: Callable[..., Any]) -> Template:
            n = name or _kebab(getattr(f, "__name__", ""))
            return self._define(n, Template(self, n, f, doc, comment))

        return make(func) if func is not None else make

    def path_template(
        self,
        func: Optional[Callable[..., Any]] = None,
        *,
        name: Optional[str] = None,
        doc: Optional[str] = None,
        comment: Optional[str] = None,
    ) -> Any:
        """Decorate a function returning a path as a path template."""

        def make(f: Callable[..., Any]) -> PathTemplate:
            n = name or _kebab(getattr(f, "__name__", ""))
            return self._define(n, PathTemplate(self, n, f, doc, comment))

        return make(func) if func is not None else make

    def uses(self, other: Union[Module, str], *, alias: Optional[str] = None) -> Module:
        """Import `other`, a module or an RDF vocabulary IRI, optionally as `alias`.

        Modules whose definitions this one uses are imported anyway; `uses`
        is for an alias (``vav41.zone-temperature``), or for RDF such as the
        Brick ontology, whose classes ``check`` then verifies.
        """
        if alias is not None:
            _check_name(alias, "alias")
        if isinstance(other, Module):
            self._aliases[other] = alias
        self._uses.append((other, alias))
        return self

    # writing

    def _render(self) -> Tuple[str, List[Module]]:
        ctx = _Ctx(self)
        body: List[str] = []
        paths: List[str] = []
        for d in self._defs:
            text = "\n".join(d._render(ctx))
            (paths if isinstance(d, (Path, PathTemplate)) else body).append(text)
        head = _comment_lines(self._comment)
        if self._base is not None:
            head.append(f"base <{self._base}>")
        imported: List[Module] = []
        for other, alias in self._uses:
            suffix = f" as {alias}" if alias else ""
            if isinstance(other, Module):
                ctx.imports.pop(other, None)
                imported.append(other)
                head.append(f'import "./{other.filename}"{suffix}')
            else:
                head.append(f"import <{other}>{suffix}")
        for other, alias in ctx.imports.items():
            imported.append(other)
            head.append(
                f'import "./{other.filename}"' + (f" as {alias}" if alias else "")
            )
        head += [f"prefix {ns.prefix}: <{ns.iri}>" for ns in ctx.namespaces.values()]
        text = "\n".join(head) + "\n\n" + "\n\n".join(paths + body) + "\n"
        return text, imported

    def source(self) -> str:
        """The module as ``.shm`` text, formatted as ``shifty shape fmt`` does."""
        text, _ = self._render()
        return _shifty._shapeshift_format(text, self.filename)

    def __str__(self) -> str:
        return self.source()

    def closure(self) -> List[Module]:
        """This module and every module it imports, directly or not."""
        out: List[Module] = []
        todo: List[Module] = [self]
        while todo:
            m = todo.pop(0)
            if m not in out:
                out.append(m)
                todo += m._render()[1]
        return out

    def write(self, directory: Union[str, pathlib.Path]) -> pathlib.Path:
        """Write this module and the modules it imports into `directory`."""
        return write(directory, self)[0]

    def check(
        self, catalog: Optional[Dict[str, Union[str, pathlib.Path]]] = None
    ) -> List[Diagnostic]:
        """Every diagnostic ``shifty shape check`` reports for this module.

        `catalog` maps imported RDF IRIs to local files, as ``--map`` does.
        """
        with tempfile.TemporaryDirectory() as d:
            write(d, self)
            raw = _shifty._shapeshift_check(
                self.source(), self.filename, d, _catalog(catalog)
            )
        return [Diagnostic(*r) for r in raw]

    def export(
        self,
        *,
        core: bool = False,
        catalog: Optional[Dict[str, Union[str, pathlib.Path]]] = None,
    ) -> str:
        """The module raised to SHACL Turtle, for :func:`shifty.validate`.

        `core` keeps to SHACL Core (no SHACL-SPARQL or extensions).
        """
        with tempfile.TemporaryDirectory() as d:
            write(d, self)
            ttl, _warnings = _shifty._shapeshift_export(
                self.source(), self.filename, d, _catalog(catalog), core
            )
        return ttl

    def __repr__(self) -> str:
        return f"<Module {self._name}>"


def _catalog(
    c: Optional[Dict[str, Union[str, pathlib.Path]]],
) -> Optional[Dict[str, str]]:
    return (
        None if c is None else {k: str(pathlib.Path(v).resolve()) for k, v in c.items()}
    )


def write(directory: Union[str, pathlib.Path], *modules: Module) -> List[pathlib.Path]:
    """Write `modules`, and the modules they import, into `directory`.

    Returns the paths written, the given modules' first.
    """
    out = pathlib.Path(directory)
    out.mkdir(parents=True, exist_ok=True)
    order: List[Module] = []
    for m in modules:
        for dep in m.closure():
            if dep not in order:
                order.append(dep)
    paths = []
    for m in order:
        p = out / m.filename
        p.write_text(m.source(), encoding="utf-8")
        paths.append(p)
    return paths


def format(source: str, label: str = "module.shm") -> str:
    """Canonical ``.shm`` text for `source`, as ``shifty shape fmt`` writes it."""
    return _shifty._shapeshift_format(source, label)
