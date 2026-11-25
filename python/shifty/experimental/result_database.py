"""Project full and partial shape-map results into SQLite."""

from __future__ import annotations

import re
import sqlite3
from collections.abc import Iterable, Iterator, Sequence
from contextlib import contextmanager
from dataclasses import dataclass
from itertools import product
from pathlib import Path
from typing import Optional
from urllib.parse import unquote, urlsplit

from shifty import EvidenceSession, GraphInputs, _coalesce_graph_input, _to_rdf_input
from shifty._shifty import _result_database_snapshot
from shifty.shapemap import Mapping, ShapeMap

__all__ = ["ResultDatabase"]


class ResultDatabase:
    """A SQLite database whose result schema is authored in SHACL.

    Accepts a filename, ``:memory:``, or a local ``sqlite:///`` URL. Use four
    slashes for an absolute path, for example ``sqlite:////tmp/results.sqlite``.
    Shape registrations persist in file-backed databases.

    Each shape gets one table with a nullable column per property. A focus
    gets one row per combination of qualifying values, with a primary key of
    ``(focus, row_number)``. Fields with no qualifying values appear as NULL.

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
        base: Optional[str] = None,
    ) -> None:
        """Register a shapes graph and create empty tables for its node shapes.

        The default includes all named node shapes. Pass full shape IRIs in
        ``shape_names`` or a SPARQL ``SELECT`` over the shapes graph binding
        ``?shape``. The two selectors are additive. An empty selection creates
        zero tables. Referenced helper shapes remain part of the shapes graph.
        Table names use declared prefixes, such as ``ex:VavShape``, and keep
        the original spelling. Use the full IRI when no prefix applies.
        """
        document = _snapshot(shapes_graph, base)
        session = EvidenceSession(document.encode(), b"", infer=False)
        tables = _compile_tables(session, shape_names, shape_query)
        if not tables:
            return

        with self._transaction():
            existing = self.connection.execute(
                "SELECT name FROM sqlite_master UNION SELECT name FROM sqlite_temp_master"
            )
            _check_names(
                (table.name for table in tables), (name for (name,) in existing)
            )
            registration = self.connection.execute(
                "INSERT INTO main._shifty_result_registrations (shapes) VALUES (?)",
                (document,),
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

    def populate_shape_tables(
        self,
        data_graph: GraphInputs,
        *,
        infer: bool = True,
        graph_mode: str = "union",
        minimum_severity: str = "info",
        base: Optional[str] = None,
    ) -> None:
        """Replace every managed result row with this graph's validation results.

        A successful refresh discards all prior rows and manual SQL edits in
        the managed result tables. Failed SHACL checks still produce rows and
        qualifying values. An error during validation or loading rolls back the
        entire refresh.
        """
        with self._transaction():
            registrations = self.connection.execute(
                "SELECT id, shapes FROM main._shifty_result_registrations ORDER BY id"
            ).fetchall()
            if not registrations:
                return
            data = _snapshot(data_graph, base).encode()
            for registration, document in registrations:
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
                tables = _compile_tables(session, list(selected))
                if {table.iri: table.name for table in tables} != selected:
                    raise ValueError(
                        "registered result tables do not match the stored shapes"
                    )
                run = session.validate(
                    shape_names=list(selected), minimum_severity=minimum_severity
                )
                shape_map = ShapeMap.from_run(run, session, name_path=None)
                for table in tables:
                    table.replace(
                        self.connection, shape_map.mappings.get(table.iri, [])
                    )

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
        id INTEGER PRIMARY KEY, shapes TEXT NOT NULL
    )""",
    """CREATE TABLE IF NOT EXISTS main._shifty_result_tables (
        table_name TEXT PRIMARY KEY NOT NULL, shape_iri TEXT NOT NULL,
        registration INTEGER NOT NULL
    )""",
    """CREATE TABLE IF NOT EXISTS main._shifty_result_fields (
        table_name TEXT NOT NULL, field_name TEXT NOT NULL, source_shape TEXT NOT NULL,
        storage_table TEXT NOT NULL, scalar INTEGER NOT NULL,
        PRIMARY KEY (table_name, field_name)
    )""",
)


@dataclass(frozen=True)
class _Field:
    source_id: int
    source: str
    name: str
    scalar: bool

    @property
    def status_columns(self) -> tuple[tuple[str, str], ...]:
        return (
            (f"{self.name}_status", "TEXT NOT NULL"),
            (f"{self.name}_value_count", "INTEGER"),
            (f"{self.name}_missing", "INTEGER"),
        )


@dataclass(frozen=True)
class _Table:
    name: str
    iri: str
    fields: list[_Field]

    @property
    def columns(self) -> list[tuple[str, str]]:
        columns = [
            ("focus", "TEXT NOT NULL"),
            ("conforms", "INTEGER NOT NULL"),
            ("row_number", "INTEGER NOT NULL"),
        ]
        columns.extend((field.name, "TEXT") for field in self.fields)
        for field in self.fields:
            columns.extend(field.status_columns)
        return columns

    def create(self, db: sqlite3.Connection) -> None:
        columns = [f"{_quote(name)} {type_}" for name, type_ in self.columns]
        columns.append("PRIMARY KEY (focus, row_number)")
        db.execute(f"CREATE TABLE {_main_table(self.name)} ({', '.join(columns)})")

    def replace(self, db: sqlite3.Connection, mappings: Sequence[Mapping]) -> None:
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
                db.executemany(insert, self._rows(focus, mapping))
                seen.add(focus)

    def _rows(
        self, focus: str, mapping: Mapping
    ) -> Iterator[tuple[str | int | None, ...]]:
        bindings = {
            binding._property_source_id: binding
            for binding in mapping.bindings.values()
        }
        field_values: list[Sequence[str | None]] = []
        statuses: list[str | int | None] = []
        for field in self.fields:
            binding = bindings.get(field.source_id)
            values = binding.values if binding is not None else None
            terms = (
                sorted({value.n3() for value in values}) if values is not None else None
            )
            field_values.append(terms or (None,))
            statuses.extend(
                (
                    binding.status if binding is not None else "unavailable",
                    len(terms) if terms is not None else None,
                    binding.missing if binding is not None else None,
                )
            )
        # Property bindings are independent sets. Materialize combinations lazily.
        for row_number, values in enumerate(product(*field_values), 1):
            yield (focus, mapping.conforms, row_number, *values, *statuses)


def _compile_tables(
    session: EvidenceSession,
    shape_names: Optional[Sequence[str]],
    shape_query: Optional[str] = None,
) -> list[_Table]:
    layout = session._inner._result_database_layout(
        list(shape_names) if shape_names is not None else None, shape_query
    )
    tables = []
    for iri, name, slots in layout:
        if "\x00" in name:
            raise ValueError("SQL names cannot contain a NUL character")
        if name.lower().startswith(("_shifty_", "sqlite_")):
            raise ValueError(f"reserved SQL table name {name!r}")
        fields = []
        for index, (source_id, source, alias, label, scalar) in enumerate(slots, 1):
            field_name = _sql_name(alias, label or "", f"field_{index}")
            fields.append(_Field(source_id, source, field_name, scalar))
        table = _Table(name, iri, fields)
        _check_names((column for column, _ in table.columns), ())
        tables.append(table)
    return tables


def _check_names(names: Iterable[str], reserved: Iterable[str]) -> None:
    seen = {name.casefold() for name in reserved}
    for name in names:
        if name.casefold() in seen:
            raise ValueError(
                f"SQL name {name!r} is used more than once or already exists"
            )
        seen.add(name.casefold())


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
