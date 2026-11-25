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
        assert db.connection.execute("SELECT * FROM [ex:VavShape]").fetchall() == []

        db.populate_shape_tables(
            VAV_DATA + "ex:vav2 ex:hasPoint ex:flow2 . ex:flow2 a ex:Airflow .",
            infer=False,
        )
        assert db.connection.execute("""
            SELECT focus, row_number, conforms, supply_air_temperature, airflow
            FROM [ex:VavShape] ORDER BY focus, row_number
        """).fetchall() == [
            ("<https://example.org/vav1>", 1, 0, "<https://example.org/sat1>", None),
            (
                "<https://example.org/vav2>",
                1,
                1,
                "<https://example.org/sat2>",
                "<https://example.org/flow2>",
            ),
            (
                "<https://example.org/vav2>",
                2,
                1,
                "<https://example.org/sat2>",
                "<https://example.org/flow>",
            ),
        ]
        assert db.connection.execute("""
            SELECT airflow_status, airflow_value_count, airflow_missing
            FROM [ex:VavShape] ORDER BY focus, row_number
        """).fetchall() == [("fail", 0, 1), ("pass", 2, 0), ("pass", 2, 0)]

        with pytest.raises(sqlite3.IntegrityError, match="UNIQUE"):
            db.connection.execute(
                "INSERT INTO [ex:VavShape] SELECT * FROM [ex:VavShape] LIMIT 1"
            )


def test_independent_repeatable_properties_share_one_shape_table():
    shapes = VAV_SHAPES + "ex:VavShape sh:property [ sh:path ex:damper ] ."
    data = (
        VAV_DATA
        + """
    ex:vav2 ex:hasPoint ex:flow2 ; ex:damper ex:d1, ex:d2, ex:d3 .
    ex:flow2 a ex:Airflow .
    """
    )
    with ResultDatabase() as db:
        db.add_shape_tables(shapes)
        db.populate_shape_tables(data, infer=False)
        assert db.connection.execute(
            "SELECT name FROM sqlite_master WHERE type = 'table' AND name LIKE 'ex:%'"
        ).fetchall() == [("ex:VavShape",)]
        assert db.connection.execute(
            "SELECT airflow, damper FROM [ex:VavShape] WHERE focus = ? ORDER BY airflow, damper",
            ("<https://example.org/vav2>",),
        ).fetchall() == [
            (f"<https://example.org/{sensor}>", f"<https://example.org/{damper}>")
            for sensor in ("flow2", "flow")
            for damper in ("d1", "d2", "d3")
        ]


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
        ).fetchall() == [
            ("ex:Extra",),
            ("ex:Helper",),
            ("ex:Implicit",),
            ("ex:Marked",),
        ]
        db.populate_shape_tables(PREFIXES)
        assert db.connection.execute("SELECT * FROM [ex:Helper]").fetchall() == []

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
        ).fetchall() == [("ex:Implicit",), ("ex:Marked",)]
        db.populate_shape_tables(PREFIXES)
        # The unselected Helper still participates through sh:node.
        assert db.connection.execute(
            "SELECT conforms FROM [ex:Implicit]"
        ).fetchall() == [(0,)]


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
            "SELECT label, conforms FROM [ex:MeterShape]"
        ).fetchall() == [('"original"', 0)]
        db.connection.execute(
            "UPDATE [ex:VavShape] SET supply_air_temperature = 'manual'"
        )
        db.connection.execute("DELETE FROM [ex:VavShape] WHERE airflow IS NOT NULL")
        db.connection.execute("""
            INSERT INTO [ex:MeterShape] (focus, conforms, row_number, label_status)
            VALUES ('manual', 1, 1, 'pass')
        """)
        db.connection.execute("CREATE TABLE notes (note TEXT)")
        db.connection.execute("INSERT INTO notes VALUES ('keep')")

    shapes_path.unlink()
    with ResultDatabase(f"sqlite:///{path}") as db:
        db.populate_shape_tables(VAV_DATA + 'ex:new a ex:Meter ; ex:label "fresh" .')
        assert db.connection.execute(
            "SELECT focus, conforms, row_number, label FROM [ex:MeterShape]"
        ).fetchall() == [("<https://example.org/new>", 1, 1, '"fresh"')]
        assert db.connection.execute(
            "SELECT supply_air_temperature, airflow FROM [ex:VavShape] ORDER BY focus"
        ).fetchall() == [
            ("<https://example.org/sat1>", None),
            ("<https://example.org/sat2>", "<https://example.org/flow>"),
        ]
        assert db.connection.execute("SELECT * FROM notes").fetchall() == [("keep",)]


def test_failed_refresh_rolls_back_every_registration():
    with ResultDatabase() as db:
        db.add_shape_tables(VAV_SHAPES)
        db.add_shape_tables(PREFIXES + "ex:Meter sh:targetClass ex:MeterClass .")
        db.populate_shape_tables(VAV_DATA + "ex:meter a ex:MeterClass .")
        db.connection.execute("""
            CREATE TRIGGER reject_refresh BEFORE INSERT ON [ex:Meter] BEGIN
                SELECT RAISE(ABORT, 'test load failure');
            END
        """)

        with pytest.raises(sqlite3.IntegrityError, match="test load failure"):
            db.populate_shape_tables(PREFIXES + "ex:new a ex:MeterClass .")

        assert db.connection.execute(
            "SELECT count(*) FROM [ex:VavShape]"
        ).fetchone() == (2,)
        assert db.connection.execute(
            "SELECT sum(airflow_missing) FROM [ex:VavShape]"
        ).fetchone() == (1,)
        assert db.connection.execute("SELECT * FROM [ex:Meter]").fetchall() == [
            ("<https://example.org/meter>", 1, 1)
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
            'SELECT "select", conforms, select_status, select_value_count '
            'FROM "Points" ORDER BY row_number'
        ).fetchall() == [
            ('"https://example.org/value"', 0, "fail", 2),
            ("<https://example.org/value>", 0, "fail", 2),
        ]

        db.populate_shape_tables(PREFIXES + r'ex:focus ex:p "line\rbreak" .')
        assert db.connection.execute(
            'SELECT "select", conforms, row_number FROM "Points"'
        ).fetchall() == [(r'"line\rbreak"', 1, 1)]


def test_table_names_use_declared_prefixes_and_preserve_spelling():
    shapes = (
        PREFIXES
        + """
    @prefix zone: <https://example.org/zones/> .
    @prefix : <https://default.example/> .
    ex:VavShape sh:targetNode ex:vav .
    <https://example.org/zones/VavShape> sh:targetNode ex:zone .
    :vav_shape sh:targetNode ex:default .
    <https://elsewhere.example/VavShape> sh:targetNode ex:elsewhere .
    """
    )
    with ResultDatabase() as db:
        db.add_shape_tables(shapes)
        db.populate_shape_tables(PREFIXES, infer=False)
        for table, local in (
            ("ex:VavShape", "vav"),
            ("zone:VavShape", "zone"),
            (":vav_shape", "default"),
            ("https://elsewhere.example/VavShape", "elsewhere"),
        ):
            assert db.connection.execute(f"SELECT focus FROM [{table}]").fetchall() == [
                (f"<https://example.org/{local}>",)
            ]


def test_table_name_collision_during_registration():
    shapes = PREFIXES + "ex:Alpha a sh:NodeShape . ex:Zeta a sh:NodeShape ."
    with ResultDatabase() as db:
        db.connection.execute("CREATE TABLE [ex:zeta] (custom TEXT)")
        with pytest.raises(ValueError, match="SQL name 'ex:Zeta'"):
            db.add_shape_tables(shapes)
        assert (
            db.connection.execute("SELECT * FROM _shifty_result_tables").fetchall()
            == []
        )
        assert (
            db.connection.execute(
                "SELECT name FROM sqlite_master WHERE name = 'ex:Alpha'"
            ).fetchall()
            == []
        )


def test_property_name_collision_with_generated_status_column():
    shapes = VAV_SHAPES + "ex:VavShape sh:property [ sh:path ex:airflow_status ] ."
    with ResultDatabase() as db:
        with pytest.raises(ValueError, match="SQL name 'airflow_status'"):
            db.add_shape_tables(shapes)
