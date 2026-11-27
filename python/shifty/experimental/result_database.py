"""Project shape-map and SPARQL query results into SQLite."""

from __future__ import annotations

import hashlib
import json
import re
import sqlite3
from collections.abc import Iterable, Iterator, Sequence
from contextlib import contextmanager
from dataclasses import dataclass
from itertools import product
from pathlib import Path
from string import ascii_lowercase, ascii_uppercase
from typing import Optional
from urllib.parse import unquote, urlsplit

from shifty import EvidenceSession, GraphInputs, _coalesce_graph_input, _to_rdf_input
from shifty._shifty import (
    _result_database_query_columns,
    _result_database_scope_shapes,
    _result_database_snapshot,
    _ResultDatabaseQueryDataset,
)
from shifty.shapemap import (
    DEFAULT_NAME_PATHS,
    Mapping,
    NamePaths,
    ShapeMap,
    _name_paths,
)
from shifty.terms import parse as parse_term

__all__ = ["ResultDatabase"]

# SQLite folds ASCII identifier characters even when the name is quoted.
_SQLITE_IDENTIFIER_FOLD = str.maketrans(ascii_uppercase, ascii_lowercase)


class ResultDatabase:
    """A SQLite database whose result tables are defined by SHACL or SPARQL.

    Accepts a filename, ``:memory:``, or a local ``sqlite:///`` URL. Use four
    slashes for an absolute path, for example ``sqlite:////tmp/results.sqlite``.
    Shape and query registrations persist in file-backed databases.

    Each shape gets one table with ``result_id``, ``focus``, and a nullable
    column per property. A focus gets one row per combination of qualifying
    values. Fields with no qualifying values appear as NULL. Shared validation
    tables record conformance and binding status for the same snapshot, keyed
    by the same ``result_id``, which identifies one focus for one shape and
    stays the same across refreshes.

    Read results through ``connection``. Every successful refresh replaces all
    rows in the managed result tables, discarding manual SQL inserts, updates,
    and deletes. SQL edits affect SQLite only. Edit the RDF source for durable
    changes and then refresh again.
    """

    def __init__(self, database: str | Path = ":memory:") -> None:
        self.connection = sqlite3.connect(_filename(database), isolation_level=None)
        try:
            self.connection.execute("PRAGMA foreign_keys = ON")
            with self._transaction():
                for statement in _REGISTRY:
                    self.connection.execute(statement)
        except BaseException:
            self.close()
            raise

    def __enter__(self) -> ResultDatabase:
        return self

    def __exit__(self, exc_type, exc, traceback) -> None:
        self.close()

    def close(self) -> None:
        """Close this manager's SQLite connection."""
        self.connection.close()

    def add_shape_tables(
        self,
        shapes_graph: GraphInputs,
        *,
        shape_names: Optional[Sequence[str]] = None,
        shape_query: Optional[str] = None,
        name_path: NamePaths = DEFAULT_NAME_PATHS,
        base: Optional[str] = None,
    ) -> None:
        """Register a shapes graph and create empty tables for its node shapes.

        The default includes all named node shapes. Pass full shape IRIs in
        ``shape_names`` or a SPARQL ``SELECT`` over the shapes graph binding
        ``?shape``. The two selectors are additive. An empty selection creates
        zero tables. The entire shapes graph remains available to query tables
        and to evaluation of referenced helper shapes.
        Table names use declared prefixes, such as ``ex:VavShape``, and keep
        the original spelling. Use the full IRI when no prefix applies.

        ``name_path`` names the columns. It takes a SPARQL property path or an
        ordered sequence of them, evaluated from each property shape over the
        shapes graph. The first path that reaches a literal names the column.
        The default is ``("sh:name", "rdfs:label")``. When no path reaches a
        literal, the property shape's IRI or path supplies a local name.
        ``sql:columnName`` overrides all of these. The paths are stored with
        the registration so refreshes keep the same columns.
        """
        document = _snapshot(shapes_graph, base)
        session = EvidenceSession(document.encode(), b"", infer=False)
        name_paths = _name_paths(name_path)
        tables = _compile_tables(session, name_paths, shape_names, shape_query)

        with self._transaction():
            self._check_table_names(
                name for table in tables for name in (table.name, table.index_name)
            )
            # Result identifiers hash the shape IRI, so each shape feeds one table.
            for table in tables:
                existing = self.connection.execute(
                    "SELECT table_name FROM main._shifty_result_tables "
                    "WHERE shape_iri = ?",
                    (table.iri,),
                ).fetchone()
                if existing is not None:
                    raise ValueError(
                        f"shape {table.iri} already has a result table: {existing[0]}"
                    )
            # A registration with no tables still supplies RDF to SPARQL queries.
            registration = self.connection.execute(
                "INSERT INTO main._shifty_result_registrations (shapes, name_paths) "
                "VALUES (?, ?)",
                (document, json.dumps(name_paths)),
            ).lastrowid
            for table in tables:
                self.connection.execute(
                    "INSERT INTO main._shifty_result_tables VALUES (?, ?, ?)",
                    (table.name, table.iri, registration),
                )
                self.connection.executemany(
                    "INSERT INTO main._shifty_result_fields VALUES (?, ?, ?, ?, ?)",
                    (
                        (
                            table.name,
                            field.name,
                            field.source,
                            table.name,
                            field.scalar,
                        )
                        for field in table.fields
                    ),
                )
                table.create(self.connection)

    def add_query_table(
        self, name: str, query: str, *, graph_mode: str = "union"
    ) -> None:
        """Create an empty table whose columns are the projected SELECT variables.

        ``union`` queries the supplied data and all registered shapes together.
        Choose ``data`` for the supplied data graph or ``shapes`` for the union
        of the registered shapes graphs. Set any SPARQL prefixes in the query.
        """
        _validate_table_name(name)
        if graph_mode not in ("union", "data", "shapes"):
            raise ValueError("query graph_mode must be 'union', 'data', or 'shapes'")
        columns = _result_database_query_columns(query)
        _check_names(columns, ())
        schema = ", ".join(f"{_quote(column)} TEXT" for column in columns)
        with self._transaction():
            self._check_table_names((name,))
            self.connection.execute(f"CREATE TABLE {_main_table(name)} ({schema})")
            self.connection.execute(
                "INSERT INTO main._shifty_result_queries VALUES (?, ?, ?)",
                (name, query, graph_mode),
            )

    def populate_shape_tables(
        self,
        data_graph: GraphInputs,
        *,
        infer: bool = True,
        graph_mode: str = "union",
        minimum_severity: str = "info",
        base: Optional[str] = None,
    ) -> None:
        """Refresh the managed tables from a graph, including registered queries."""
        self.refresh(
            data_graph,
            infer=infer,
            graph_mode=graph_mode,
            minimum_severity=minimum_severity,
            base=base,
        )

    def refresh(
        self,
        data_graph: GraphInputs,
        *,
        infer: bool = True,
        graph_mode: str = "union",
        minimum_severity: str = "info",
        base: Optional[str] = None,
    ) -> None:
        """Replace the managed shape and query results in one transaction.

        A successful refresh discards all prior rows and manual SQL edits in
        the managed result tables. Failed SHACL checks still produce rows and
        qualifying values. An error during validation, SPARQL, or loading rolls
        back the entire refresh. ``infer`` and ``graph_mode`` control SHACL
        evaluation. ``minimum_severity`` determines which failures make a
        focus nonconforming. Query tables use the saved graph mode.
        """
        with self._transaction():
            registrations = self.connection.execute(
                "SELECT id, shapes, name_paths FROM main._shifty_result_registrations "
                "ORDER BY id"
            ).fetchall()
            queries = self.connection.execute(
                "SELECT table_name, query, graph_mode FROM main._shifty_result_queries ORDER BY table_name"
            ).fetchall()
            if not registrations and not queries:
                return
            data = _snapshot(data_graph, base).encode()
            # Both projections must see the same labels for shared blank nodes.
            shapes = (
                _result_database_scope_shapes(
                    data, [document for _, document, _ in registrations]
                )
                if registrations
                else []
            )
            # Clear the rows that reference validation results before the
            # results themselves, so the delete does not cascade row by row.
            for (name,) in self.connection.execute(
                "SELECT table_name FROM main._shifty_result_tables"
            ).fetchall():
                self.connection.execute(f"DELETE FROM {_main_table(name)}")
            self.connection.execute("DELETE FROM main._shifty_result_binding_status")
            self.connection.execute("DELETE FROM main._shifty_result_validation")
            for (registration, _, name_paths), document in zip(registrations, shapes):
                selected = dict(
                    self.connection.execute(
                        "SELECT shape_iri, table_name FROM main._shifty_result_tables "
                        "WHERE registration = ?",
                        (registration,),
                    )
                )
                if not selected:
                    continue
                session = EvidenceSession(
                    document.encode(), data, infer=infer, graph_mode=graph_mode
                )
                tables = _compile_tables(
                    session, json.loads(name_paths), list(selected)
                )
                if {table.iri: table.name for table in tables} != selected:
                    raise ValueError(
                        "registered result tables do not match the stored shapes"
                    )
                run = session.validate(
                    shape_names=list(selected), minimum_severity=minimum_severity
                )
                # The run's session can recover qualifying values omitted by
                # failure evidence, including values from passing siblings.
                shape_map = ShapeMap.from_run(run, session, name_path=None)
                for table in tables:
                    table.replace(
                        self.connection,
                        shape_map.mappings.get(table.iri, []),
                        minimum_severity,
                    )
            if queries:
                if all(mode == "data" for _, _, mode in queries):
                    shapes = []
                self._refresh_queries(data, shapes, queries)

    def _refresh_queries(
        self, data: bytes, shapes: list[str], queries: list[tuple[str, str, str]]
    ) -> None:
        dataset = _ResultDatabaseQueryDataset(data, shapes)
        for name, query, graph_mode in queries:
            columns, rows = dataset.select(query, graph_mode)
            names = ", ".join(_quote(column) for column in columns)
            placeholders = ", ".join("?" for _ in columns)
            table = _main_table(name)
            self.connection.execute(f"DELETE FROM {table}")
            # Use the same RDF term rendering as the shape-map columns.
            self.connection.executemany(
                f"INSERT INTO {table} ({names}) VALUES ({placeholders})",
                (
                    tuple(
                        parse_term(value).n3() if value is not None else None
                        for value in row
                    )
                    for row in rows
                ),
            )

    def _check_table_names(self, names: Iterable[str]) -> None:
        existing = self.connection.execute(
            "SELECT name FROM sqlite_master UNION SELECT name FROM sqlite_temp_master"
        )
        _check_names(names, (name for (name,) in existing))

    @contextmanager
    def _transaction(self) -> Iterator[None]:
        connection = self.connection
        # An explicit transaction on the exposed connection belongs to its caller.
        connection.execute("BEGIN IMMEDIATE")
        try:
            yield
            connection.execute("COMMIT")
        except BaseException:
            if connection.in_transaction:
                connection.execute("ROLLBACK")
            raise


_REGISTRY = (
    """CREATE TABLE IF NOT EXISTS main._shifty_result_registrations (
        id INTEGER PRIMARY KEY, shapes TEXT NOT NULL, name_paths TEXT NOT NULL
    )""",
    """CREATE TABLE IF NOT EXISTS main._shifty_result_tables (
        table_name TEXT PRIMARY KEY NOT NULL, shape_iri TEXT NOT NULL UNIQUE,
        registration INTEGER NOT NULL
    )""",
    """CREATE TABLE IF NOT EXISTS main._shifty_result_queries (
        table_name TEXT PRIMARY KEY NOT NULL, query TEXT NOT NULL, graph_mode TEXT NOT NULL
    )""",
    """CREATE TABLE IF NOT EXISTS main._shifty_result_fields (
        table_name TEXT NOT NULL, field_name TEXT NOT NULL, source_shape TEXT NOT NULL,
        storage_table TEXT NOT NULL, scalar INTEGER NOT NULL,
        PRIMARY KEY (table_name, field_name)
    )""",
    """CREATE TABLE IF NOT EXISTS main._shifty_result_validation (
        result_id TEXT PRIMARY KEY NOT NULL,
        table_name TEXT NOT NULL REFERENCES _shifty_result_tables (table_name) ON DELETE CASCADE,
        focus TEXT NOT NULL, conforms INTEGER NOT NULL,
        UNIQUE (table_name, focus)
    )""",
    """CREATE TABLE IF NOT EXISTS main._shifty_result_binding_status (
        result_id TEXT NOT NULL
            REFERENCES _shifty_result_validation (result_id) ON DELETE CASCADE,
        table_name TEXT NOT NULL, focus TEXT NOT NULL, field_name TEXT NOT NULL,
        status TEXT NOT NULL, value_count INTEGER, missing INTEGER,
        PRIMARY KEY (result_id, field_name)
    )""",
    """CREATE VIEW IF NOT EXISTS main._shifty_result_missing_required_values AS
        SELECT result_id, table_name, focus, field_name, missing
        FROM main._shifty_result_binding_status
        WHERE missing > 0
    """,
)


@dataclass(frozen=True)
class _Field:
    source_id: int
    source: str
    name: str
    scalar: bool


@dataclass(frozen=True)
class _Table:
    name: str
    iri: str
    fields: list[_Field]

    @property
    def columns(self) -> list[tuple[str, str]]:
        columns = [
            (
                "result_id",
                "TEXT NOT NULL REFERENCES _shifty_result_validation (result_id) "
                "ON DELETE CASCADE",
            ),
            ("focus", "TEXT NOT NULL"),
        ]
        columns.extend((field.name, "TEXT") for field in self.fields)
        return columns

    @property
    def index_name(self) -> str:
        return f"_shifty_result_index:{self.name}"

    def create(self, db: sqlite3.Connection) -> None:
        columns = [f"{_quote(name)} {type_}" for name, type_ in self.columns]
        db.execute(f"CREATE TABLE {_main_table(self.name)} ({', '.join(columns)})")
        # Joins from validation results, and their cascading deletes, look up
        # shape rows by result_id.
        db.execute(
            f"CREATE INDEX {_main_table(self.index_name)} "
            f"ON {_quote(self.name)} (result_id)"
        )

    def replace(
        self, db: sqlite3.Connection, mappings: Sequence[Mapping], minimum_severity: str
    ) -> None:
        name = _main_table(self.name)
        columns = self.columns
        names = ", ".join(_quote(column) for column, _ in columns)
        placeholders = ", ".join("?" for _ in columns)
        insert = f"INSERT INTO {name} ({names}) VALUES ({placeholders})"
        db.execute(f"DELETE FROM {name}")

        # A focus may be selected by multiple targets on the same node shape.
        seen: set[str] = set()
        for mapping in mappings:
            focus = mapping.focus.n3()
            if focus not in seen:
                # Apply the threshold to conformance while saving raw binding status.
                failure = mapping.evaluation.failure
                conforms = failure is None or not failure._meets_threshold(
                    minimum_severity
                )
                self._insert_focus(db, insert, focus, mapping, conforms)
                seen.add(focus)

    def _insert_focus(
        self,
        db: sqlite3.Connection,
        insert: str,
        focus: str,
        mapping: Mapping,
        conforms: bool,
    ) -> None:
        # Distinct property shapes can share a path but bind different values.
        bindings = {
            binding._property_source_id: binding
            for binding in mapping.bindings.values()
        }
        result_id = _result_id(self.iri, focus)
        field_values: list[Sequence[str | None]] = []
        binding_rows = []
        for field in self.fields:
            binding = bindings.get(field.source_id)
            values = binding.values if binding is not None else None
            terms = (
                sorted({value.n3() for value in values}) if values is not None else None
            )
            # A NULL keeps this focus in the product when a field has no values.
            # The diagnostic count still distinguishes empty from unavailable.
            field_values.append(terms or (None,))
            binding_rows.append(
                (
                    result_id,
                    self.name,
                    focus,
                    field.name,
                    binding.status if binding is not None else "unavailable",
                    len(terms) if terms is not None else None,
                    binding.missing if binding is not None else None,
                )
            )
        db.execute(
            "INSERT INTO main._shifty_result_validation "
            "(result_id, table_name, focus, conforms) VALUES (?, ?, ?, ?)",
            (result_id, self.name, focus, conforms),
        )
        db.executemany(
            "INSERT INTO main._shifty_result_binding_status VALUES (?, ?, ?, ?, ?, ?, ?)",
            binding_rows,
        )
        # Property bindings are independent sets. Materialize combinations lazily.
        db.executemany(
            insert,
            ((result_id, focus, *values) for values in product(*field_values)),
        )


def _compile_tables(
    session: EvidenceSession,
    name_paths: list[str],
    shape_names: Optional[Sequence[str]],
    shape_query: Optional[str] = None,
) -> list[_Table]:
    layout = session._inner._result_database_layout(
        name_paths, list(shape_names) if shape_names is not None else None, shape_query
    )
    tables = []
    for iri, name, slots in layout:
        _validate_table_name(name)
        fields = []
        for index, (source_id, source, alias, label, scalar) in enumerate(slots, 1):
            field_name = _sql_name(alias, label or "", f"field_{index}")
            fields.append(_Field(source_id, source, field_name, scalar))
        table = _Table(name, iri, fields)
        _check_names((column for column, _ in table.columns), ())
        tables.append(table)
    return tables


def _result_id(shape_iri: str, focus: str) -> str:
    """A stable 64-bit identifier for one focus node's result for one shape.

    The same shape and focus get the same identifier on every refresh and
    under any table name, so applications can keep it between refreshes. Each
    shape feeds one table, which keeps identifiers unique. Blank-node foci are
    relabelled per snapshot, so their identifiers are only stable within one
    refresh.
    """
    digest = hashlib.sha256(f"{shape_iri}\0{focus}".encode()).digest()
    # Sixteen hex digits keep all 64 bits, survive JSON and JavaScript, and
    # select as one word when copied.
    return digest[:8].hex()


def _validate_table_name(name: str) -> None:
    if not name.strip():
        raise ValueError("SQL table name must be nonempty")
    if "\x00" in name:
        raise ValueError("SQL names cannot contain a NUL character")
    if name.translate(_SQLITE_IDENTIFIER_FOLD).startswith(("_shifty_", "sqlite_")):
        raise ValueError(f"reserved SQL table name {name!r}")


def _check_names(names: Iterable[str], reserved: Iterable[str]) -> None:
    seen = {name.translate(_SQLITE_IDENTIFIER_FOLD) for name in reserved}
    for name in names:
        folded = name.translate(_SQLITE_IDENTIFIER_FOLD)
        if folded in seen:
            raise ValueError(
                f"SQL name {name!r} is used more than once or already exists"
            )
        seen.add(folded)


def _sql_name(alias: Optional[str], label: str, fallback: str) -> str:
    if alias is not None:
        if "\x00" in alias:
            raise ValueError("SQL names cannot contain a NUL character")
        return alias
    words = re.sub(r"([a-z0-9])([A-Z])", r"\1_\2", label)
    return re.sub(r"[^a-z0-9]+", "_", words.lower()).strip("_") or fallback


def _quote(name: str) -> str:
    return '"' + name.replace('"', '""') + '"'


def _main_table(name: str) -> str:
    return "main." + _quote(name)


def _filename(database: str | Path) -> str:
    if isinstance(database, Path) or "://" not in database:
        return str(database)
    url = urlsplit(database)
    if not database.startswith("sqlite:///") or url.netloc or url.query or url.fragment:
        raise ValueError("expected a local sqlite:/// URL")
    filename = unquote(url.path[1:])
    if not filename:
        raise ValueError("SQLite URL must name a database file or :memory:")
    return filename


def _snapshot(graph: GraphInputs, base: Optional[str]) -> str:
    source = _to_rdf_input(_coalesce_graph_input(graph))
    return _result_database_snapshot(source.data, source.path, source.format, base)
