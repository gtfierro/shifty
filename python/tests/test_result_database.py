import sqlite3

import pytest

from shifty.experimental.result_database import ResultDatabase

PREFIXES = """
@prefix ex: <https://example.org/> .
@prefix sh: <http://www.w3.org/ns/shacl#> .
@prefix sql: <https://shifty.gtf.fyi/ns/sql#> .
"""

VAV_SHAPES = (
    PREFIXES
    + """
ex:VavShape sh:targetClass ex:Vav ; sh:targetNode ex:vav1 ;
    sh:property [
        sh:path ex:hasPoint ; sh:name "supply air temperature" ;
        sh:qualifiedValueShape [ sh:class ex:Temperature ] ;
        sh:qualifiedMinCount 1 ; sh:qualifiedMaxCount 1
    ], [
        sh:path ex:hasPoint ; sh:name "airflow" ;
        sh:qualifiedValueShape [ sh:class ex:Airflow ] ; sh:qualifiedMinCount 1
    ] .
"""
)

VAV_DATA = (
    PREFIXES
    + """
ex:vav1 a ex:Vav ; ex:hasPoint ex:sat1 .
ex:vav2 a ex:Vav ; ex:hasPoint ex:sat2, ex:flow .
ex:sat1 a ex:Temperature .
ex:sat2 a ex:Temperature .
ex:flow a ex:Airflow .
"""
)


def test_projects_full_and_partial_results():
    with ResultDatabase() as db:
        db.add_shape_tables(VAV_SHAPES)
        assert db.connection.execute("SELECT * FROM vav_shape").fetchall() == []

        db.populate_shape_tables(VAV_DATA, infer=False)
        assert db.connection.execute(
            "SELECT focus, supply_air_temperature, conforms FROM vav_shape ORDER BY focus"
        ).fetchall() == [
            ("<https://example.org/vav1>", "<https://example.org/sat1>", 0),
            ("<https://example.org/vav2>", "<https://example.org/sat2>", 1),
        ]
        assert db.connection.execute("SELECT * FROM vav_shape_airflow").fetchall() == [
            ("<https://example.org/vav2>", "<https://example.org/flow>")
        ]
        assert db.connection.execute(
            "SELECT status, value_count, missing FROM vav_shape_slot_status "
            "WHERE field = 'airflow' ORDER BY focus"
        ).fetchall() == [("fail", 0, 1), ("pass", 1, 0)]


def test_default_and_explicit_shape_selection():
    shapes = (
        PREFIXES
        + """
    ex:Implicit sh:targetNode ex:focus ; sh:node ex:Helper .
    ex:Marked a sh:NodeShape ; ex:include true ; sh:targetNode ex:other .
    ex:Extra a sh:NodeShape .
    ex:Helper a sh:NodeShape ; sh:hasValue ex:valid .
    ex:Property a sh:PropertyShape ; sh:path ex:p ; sh:targetNode ex:focus .
    [] a sh:NodeShape ; sh:targetNode ex:focus .
    """
    )
    with ResultDatabase() as db:
        db.add_shape_tables(shapes)
        assert db.connection.execute(
            "SELECT table_name FROM _shifty_result_tables ORDER BY table_name"
        ).fetchall() == [("extra",), ("helper",), ("implicit",), ("marked",)]
        db.populate_shape_tables(PREFIXES)
        assert db.connection.execute("SELECT * FROM helper").fetchall() == []

    with ResultDatabase("sqlite:///:memory:") as db:
        db.add_shape_tables(shapes, shape_names=[])
        db.add_shape_tables(
            shapes, shape_query="SELECT ?shape WHERE { ?shape ex:absent true }"
        )
        assert (
            db.connection.execute("SELECT * FROM _shifty_result_tables").fetchall()
            == []
        )
        db.add_shape_tables(
            shapes,
            shape_names=["https://example.org/Implicit", "https://example.org/Marked"],
            shape_query="SELECT ?shape WHERE { ?shape ex:include true }",
        )
        assert db.connection.execute(
            "SELECT table_name FROM _shifty_result_tables ORDER BY table_name"
        ).fetchall() == [("implicit",), ("marked",)]
        db.populate_shape_tables(PREFIXES)
        # The unselected Helper still participates through sh:node.
        assert db.connection.execute("SELECT conforms FROM implicit").fetchall() == [
            (0,)
        ]


@pytest.mark.parametrize(
    ("selectors", "error"),
    [
        ({"shape_names": ["https://example.org/Point"]}, "expected a named node shape"),
        ({"shape_query": "ASK {}"}, "must be a SPARQL SELECT"),
        ({"shape_query": "SELECT ?wrong WHERE { ?wrong ?p ?o }"}, "must project"),
        (
            {"shape_query": 'SELECT ?shape WHERE { VALUES ?shape { "literal" } }'},
            "to an IRI",
        ),
    ],
)
def test_invalid_selectors(selectors, error):
    with ResultDatabase() as db:
        with pytest.raises(ValueError, match=error):
            db.add_shape_tables(PREFIXES + "ex:Point sh:path ex:p .", **selectors)
        assert (
            db.connection.execute("SELECT * FROM _shifty_result_tables").fetchall()
            == []
        )


def test_reopen_and_refresh_replaces_only_managed_rows(tmp_path):
    path = tmp_path / "building.sqlite"
    shapes_path = tmp_path / "shapes.ttl"
    shapes_path.write_text(
        PREFIXES
        + """
        @base <https://example.org/> .
        ex:MeterShape sh:targetClass ex:Meter ;
            sh:sparql [ sh:select "SELECT $this WHERE { $this ex:invalid <bad> }" ] ;
            sh:property [ sh:path ex:label ; sh:maxCount 1 ] .
        """,
        encoding="utf-8",
    )
    with ResultDatabase(path) as db:
        db.add_shape_tables(VAV_SHAPES)
        db.add_shape_tables(shapes_path)
        db.populate_shape_tables(
            VAV_DATA + 'ex:old a ex:Meter ; ex:label "original" ; ex:invalid ex:bad .'
        )
        assert db.connection.execute(
            "SELECT label, conforms FROM meter_shape"
        ).fetchall() == [('"original"', 0)]
        db.connection.execute("UPDATE vav_shape SET supply_air_temperature = 'manual'")
        db.connection.execute("DELETE FROM vav_shape_airflow")
        db.connection.execute("INSERT INTO meter_shape VALUES ('manual', 1, NULL)")
        db.connection.execute("CREATE TABLE notes (note TEXT)")
        db.connection.execute("INSERT INTO notes VALUES ('keep')")

    shapes_path.unlink()
    with ResultDatabase(f"sqlite:///{path}") as db:
        db.populate_shape_tables(VAV_DATA + 'ex:new a ex:Meter ; ex:label "fresh" .')
        assert db.connection.execute("SELECT * FROM meter_shape").fetchall() == [
            ("<https://example.org/new>", 1, '"fresh"')
        ]
        assert db.connection.execute(
            "SELECT value FROM vav_shape_airflow"
        ).fetchall() == [("<https://example.org/flow>",)]
        assert db.connection.execute(
            "SELECT supply_air_temperature FROM vav_shape ORDER BY focus"
        ).fetchall() == [
            ("<https://example.org/sat1>",),
            ("<https://example.org/sat2>",),
        ]
        assert db.connection.execute("SELECT * FROM notes").fetchall() == [("keep",)]


def test_failed_refresh_rolls_back_every_registration():
    with ResultDatabase() as db:
        db.add_shape_tables(VAV_SHAPES)
        db.add_shape_tables(PREFIXES + "ex:Meter sh:targetClass ex:MeterClass .")
        db.populate_shape_tables(VAV_DATA + "ex:meter a ex:MeterClass .")
        db.connection.execute("""
            CREATE TRIGGER reject_refresh BEFORE INSERT ON meter BEGIN
                SELECT RAISE(ABORT, 'test load failure');
            END
        """)

        with pytest.raises(sqlite3.IntegrityError, match="test load failure"):
            db.populate_shape_tables(PREFIXES + "ex:new a ex:MeterClass .")

        assert db.connection.execute("SELECT count(*) FROM vav_shape").fetchone() == (
            2,
        )
        assert db.connection.execute(
            "SELECT count(*) FROM vav_shape_slot_status"
        ).fetchone() == (4,)
        assert db.connection.execute("SELECT * FROM meter").fetchall() == [
            ("<https://example.org/meter>", 1)
        ]


def test_scalar_overflow_and_authored_aliases():
    shapes = (
        PREFIXES
        + """
    ex:Shape sh:targetNode ex:focus ; sql:tableName "Points" ; sh:property [
        sh:path ex:p ; sh:maxCount 1 ; sql:columnName "select"
    ] .
    """
    )
    with ResultDatabase() as db:
        db.add_shape_tables(shapes)
        db.populate_shape_tables(
            PREFIXES + 'ex:focus ex:p ex:value, "https://example.org/value" .'
        )
        assert db.connection.execute(
            'SELECT "select", conforms FROM "Points"'
        ).fetchall() == [(None, 0)]
        assert db.connection.execute(
            'SELECT field, value FROM "Points_scalar_values" ORDER BY value'
        ).fetchall() == [
            ("select", '"https://example.org/value"'),
            ("select", "<https://example.org/value>"),
        ]
        assert db.connection.execute(
            'SELECT status, value_count FROM "Points_slot_status"'
        ).fetchall() == [("fail", 2)]

        db.populate_shape_tables(PREFIXES + r'ex:focus ex:p "line\rbreak" .')
        assert db.connection.execute(
            'SELECT "select", conforms FROM "Points"'
        ).fetchall() == [(r'"line\rbreak"', 1)]
        assert (
            db.connection.execute('SELECT * FROM "Points_scalar_values"').fetchall()
            == []
        )


def test_table_name_collision_during_registration():
    shapes = PREFIXES + "ex:Alpha a sh:NodeShape . ex:Zeta a sh:NodeShape ."
    with ResultDatabase() as db:
        db.connection.execute("CREATE TABLE zeta (custom TEXT)")
        with pytest.raises(ValueError, match="SQL name 'zeta'"):
            db.add_shape_tables(shapes)
        assert (
            db.connection.execute("SELECT * FROM _shifty_result_tables").fetchall()
            == []
        )
        assert (
            db.connection.execute(
                "SELECT name FROM sqlite_master WHERE name = 'alpha'"
            ).fetchall()
            == []
        )
