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
        empty = db.connection.execute("SELECT * FROM [ex:VavShape]")
        assert empty.fetchall() == []
        assert {column[0] for column in empty.description} == {
            "focus",
            "supply_air_temperature",
            "airflow",
        }

        db.populate_shape_tables(
            VAV_DATA + "ex:vav2 ex:hasPoint ex:flow2 . ex:flow2 a ex:Airflow .",
            infer=False,
        )
        assert db.connection.execute("""
            SELECT focus, supply_air_temperature, airflow
            FROM [ex:VavShape] ORDER BY focus, airflow
        """).fetchall() == [
            ("<https://example.org/vav1>", "<https://example.org/sat1>", None),
            (
                "<https://example.org/vav2>",
                "<https://example.org/sat2>",
                "<https://example.org/flow2>",
            ),
            (
                "<https://example.org/vav2>",
                "<https://example.org/sat2>",
                "<https://example.org/flow>",
            ),
        ]
        assert db.connection.execute("""
            SELECT focus, conforms FROM _shifty_result_validation
            WHERE table_name = 'ex:VavShape' ORDER BY focus
        """).fetchall() == [
            ("<https://example.org/vav1>", 0),
            ("<https://example.org/vav2>", 1),
        ]
        assert db.connection.execute("""
            SELECT focus, status, value_count, missing FROM _shifty_result_binding_status
            WHERE table_name = 'ex:VavShape' AND field_name = 'airflow' ORDER BY focus
        """).fetchall() == [
            ("<https://example.org/vav1>", "fail", 0, 1),
            ("<https://example.org/vav2>", "pass", 2, 0),
        ]
        assert db.connection.execute(
            "SELECT * FROM _shifty_result_missing_required_values"
        ).fetchall() == [("ex:VavShape", "<https://example.org/vav1>", "airflow", 1)]

        db.populate_shape_tables(
            VAV_DATA + "ex:vav1 ex:hasPoint ex:flow .", infer=False
        )
        assert (
            db.connection.execute(
                "SELECT * FROM _shifty_result_missing_required_values"
            ).fetchall()
            == []
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


def test_query_tables_use_the_registered_graph_scope():
    labels = """
        PREFIX ex: <https://example.org/>
        SELECT ?item ?label WHERE { ?item ex:label ?label }
    """
    with ResultDatabase() as db:
        db.add_shape_tables(
            VAV_SHAPES + 'ex:VavShape ex:label "shape" . ex:shared ex:label "both" .'
        )
        db.add_shape_tables(
            PREFIXES + 'ex:AdditionalProfile ex:label "additional" .', shape_names=[]
        )
        db.add_query_table("all_labels", labels)
        db.add_query_table("data_labels", labels, graph_mode="data")
        db.add_query_table("shape_labels", labels, graph_mode="shapes")
        db.add_query_table(
            "vav_profiles",
            """
            PREFIX ex: <https://example.org/>
            SELECT ?focus ?label ?room WHERE {
                ?focus ex:usesProfile/ex:label ?label .
                OPTIONAL { ?focus ex:room ?room }
            }
            """,
        )
        empty = db.connection.execute("SELECT * FROM vav_profiles")
        assert [column[0] for column in empty.description] == ["focus", "label", "room"]
        assert empty.fetchall() == []

        db.refresh(
            VAV_DATA + 'ex:vav1 ex:usesProfile ex:VavShape ; ex:label "data" . '
            'ex:shared ex:label "both" .',
            infer=False,
            graph_mode="data",
        )
        for table, expected in (
            ("all_labels", ['"additional"', '"both"', '"data"', '"shape"']),
            ("data_labels", ['"both"', '"data"']),
            ("shape_labels", ['"additional"', '"both"', '"shape"']),
        ):
            assert db.connection.execute(
                f"SELECT label FROM {table} ORDER BY label"
            ).fetchall() == [(label,) for label in expected]
        assert db.connection.execute("""
            SELECT v.focus, q.label, q.room FROM [ex:VavShape] AS v
            JOIN vav_profiles AS q USING (focus)
        """).fetchall() == [("<https://example.org/vav1>", '"shape"', None)]


def test_query_tables_preserve_rdf_identity_across_graphs():
    shapes = (
        PREFIXES
        + """
        ex:SensorShape sh:targetClass ex:Sensor ; sh:property [ sh:path ex:code ] .
        _:shared ex:label "shape1" .
    """
    )
    data = (
        PREFIXES
        + """
        _:shared a ex:Sensor ; ex:label "data" ;
            ex:code "01"^^<http://www.w3.org/2001/XMLSchema#integer> .
    """
    )
    query = """
        PREFIX ex: <https://example.org/>
        SELECT ?focus ?label ?code WHERE {
            ?focus ex:label ?label . OPTIONAL { ?focus ex:code ?code }
        }
    """
    with ResultDatabase() as db:
        db.add_shape_tables(shapes)
        db.add_shape_tables(PREFIXES + '_:shared ex:label "shape2" .', shape_names=[])
        db.add_query_table("combined", query)
        db.add_query_table("shape_terms", query, graph_mode="shapes")
        db.refresh(data, infer=False)

        assert db.connection.execute(
            "SELECT COUNT(*), COUNT(DISTINCT focus) FROM combined"
        ).fetchone() == (3, 3)
        assert db.connection.execute("""
            SELECT label FROM combined JOIN shape_terms USING (focus, label) ORDER BY label
        """).fetchall() == [('"shape1"',), ('"shape2"',)]
        assert db.connection.execute("""
            SELECT label, code FROM [ex:SensorShape] JOIN combined USING (focus, code)
        """).fetchall() == [
            ('"data"', '"01"^^<http://www.w3.org/2001/XMLSchema#integer>')
        ]


def test_shape_blank_nodes_join_to_query_tables():
    first = (
        PREFIXES
        + """
        ex:FirstShape sh:targetNode ex:node ; sh:property [ sh:path ex:first ] .
        ex:node ex:first _:shared .
        _:shared ex:label "first" .
    """
    )
    second = (
        PREFIXES
        + """
        ex:SecondShape sh:targetNode ex:node ; sh:property [ sh:path ex:second ] .
        ex:node ex:second _:shared .
        _:shared ex:label "second" .
    """
    )
    with ResultDatabase() as db:
        db.add_shape_tables(first)
        db.add_shape_tables(second)
        db.add_query_table(
            "labels",
            "SELECT ?node ?label WHERE { ?node <https://example.org/label> ?label }",
        )
        db.refresh(PREFIXES + '_:shared ex:label "data" . ex:node a ex:Example .')

        assert db.connection.execute("""
            SELECT label FROM [ex:FirstShape] JOIN labels ON first = node
            UNION ALL
            SELECT label FROM [ex:SecondShape] JOIN labels ON second = node
            ORDER BY label
        """).fetchall() == [('"first"',), ('"second"',)]
        assert db.connection.execute(
            "SELECT first != second FROM [ex:FirstShape] JOIN [ex:SecondShape] USING (focus)"
        ).fetchall() == [(1,)]


def test_result_tables_preserve_escaped_literal_values():
    shapes = (
        PREFIXES + "ex:Shape sh:targetNode ex:item ; sh:property [ sh:path ex:p ] ."
    )
    literals = (
        r'"\b"',
        r'"\\b"',
        r'"\f"',
        r'"\\f"',
        r'"\t"',
        r'"\\t"',
        r'"\u0001"',
        r'"\\u0001"',
    )
    data = PREFIXES + f"ex:item ex:p {', '.join(literals)} ."
    with ResultDatabase() as db:
        db.add_shape_tables(shapes)
        db.add_query_table(
            "projected",
            "SELECT ?p WHERE { <https://example.org/item> <https://example.org/p> ?p }",
        )
        db.refresh(data, infer=False)
        expected = [(literal,) for literal in sorted(literals)]
        assert (
            db.connection.execute("SELECT p FROM [ex:Shape] ORDER BY p").fetchall()
            == expected
        )
        assert (
            db.connection.execute("SELECT p FROM projected ORDER BY p").fetchall()
            == expected
        )


def test_refresh_applies_the_severity_threshold_per_focus():
    shapes = (
        PREFIXES
        + """
        ex:DeviceShape sh:targetClass ex:Device ; sh:property [
            sh:path ex:label ; sh:minCount 1 ; sh:severity sh:Warning
        ], [sh:path ex:code ; sh:minCount 1] .
    """
    )
    data = (
        PREFIXES
        + 'ex:warn a ex:Device ; ex:code "code" . ex:error a ex:Device ; ex:label "label" .'
    )
    select = "SELECT focus, conforms FROM _shifty_result_validation ORDER BY focus"
    with ResultDatabase() as db:
        db.add_shape_tables(shapes)
        db.refresh(data, minimum_severity="violation")
        assert db.connection.execute(select).fetchall() == [
            ("<https://example.org/error>", 0),
            ("<https://example.org/warn>", 1),
        ]
        assert db.connection.execute("""
            SELECT status, missing FROM _shifty_result_binding_status
            WHERE focus = '<https://example.org/warn>' AND field_name = 'label'
        """).fetchall() == [("fail", 1)]

        db.refresh(data, minimum_severity="warning")
        assert db.connection.execute(select).fetchall() == [
            ("<https://example.org/error>", 0),
            ("<https://example.org/warn>", 0),
        ]


def test_table_and_column_names_use_sqlite_ascii_case_folding():
    shapes = (
        PREFIXES
        + """
        ex:Shape sh:targetNode ex:item ; sh:property [
            sh:path ex:p ; sql:columnName "straße"
        ], [sh:path ex:q ; sql:columnName "strasse"] .
    """
    )
    with ResultDatabase() as db:
        db.add_shape_tables(shapes)
        db.add_query_table(
            "straße", 'SELECT ("a" AS ?straße) ("b" AS ?strasse) WHERE {}'
        )
        db.add_query_table("strasse", 'SELECT ("c" AS ?value) WHERE {}')
        with pytest.raises(ValueError, match="SQL name 'STRASSE'"):
            db.add_query_table("STRASSE", 'SELECT ("d" AS ?value) WHERE {}')
        db.refresh(PREFIXES + 'ex:item ex:p "a" ; ex:q "b" .')

        assert db.connection.execute(
            'SELECT "straße", strasse FROM [ex:Shape]'
        ).fetchall() == [('"a"', '"b"')]
        assert db.connection.execute(
            'SELECT "straße", strasse FROM "straße"'
        ).fetchall() == [('"a"', '"b"')]
        assert db.connection.execute("SELECT value FROM strasse").fetchall() == [
            ('"c"',)
        ]


def test_query_tables_can_refresh_without_shape_tables_after_reopening(tmp_path):
    path = tmp_path / "queries.db"
    with ResultDatabase(path) as db:
        db.add_query_table(
            "vavs",
            """
            PREFIX ex: <https://example.org/>
            SELECT DISTINCT (?vav AS ?focus) WHERE { ?vav a ex:Vav }
            ORDER BY ?focus LIMIT 3
            """,
            graph_mode="data",
        )
        db.refresh(VAV_DATA)
        assert db.connection.execute("SELECT COUNT(*) FROM vavs").fetchone() == (2,)
        db.connection.execute("INSERT INTO vavs VALUES ('manual')")

    with ResultDatabase(path) as db:
        db.populate_shape_tables(PREFIXES + "ex:new a ex:Vav .")
        assert db.connection.execute("SELECT * FROM vavs").fetchall() == [
            ("<https://example.org/new>",)
        ]


def test_invalid_query_tables_leave_the_database_unchanged():
    with ResultDatabase() as db:
        db.add_shape_tables(VAV_SHAPES)
        with pytest.raises(ValueError, match="graph_mode"):
            db.add_query_table(
                "example", "SELECT ?s WHERE { ?s ?p ?o }", graph_mode="unknown"
            )
        with pytest.raises(ValueError, match="requires a SPARQL SELECT"):
            db.add_query_table("example", "ASK { ?s ?p ?o }")
        with pytest.raises(ValueError, match="at least one projected variable"):
            db.add_query_table("example", "SELECT * WHERE {}")
        with pytest.raises(ValueError, match="SQL name 'ex:VavShape'"):
            db.add_query_table("ex:VavShape", "SELECT ?s WHERE { ?s ?p ?o }")
        assert (
            db.connection.execute("SELECT * FROM _shifty_result_queries").fetchall()
            == []
        )


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
            "SELECT conforms FROM _shifty_result_validation WHERE table_name = 'ex:Implicit'"
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
            "SELECT label FROM [ex:MeterShape]"
        ).fetchall() == [('"original"',)]
        assert db.connection.execute(
            "SELECT conforms FROM _shifty_result_validation WHERE table_name = 'ex:MeterShape'"
        ).fetchall() == [(0,)]
        db.connection.execute(
            "UPDATE [ex:VavShape] SET supply_air_temperature = 'manual'"
        )
        db.connection.execute("DELETE FROM [ex:VavShape] WHERE airflow IS NOT NULL")
        db.connection.execute("INSERT INTO [ex:MeterShape] (focus) VALUES ('manual')")
        db.connection.execute("""
            UPDATE _shifty_result_binding_status SET missing = 100
            WHERE table_name = 'ex:VavShape' AND field_name = 'airflow'
        """)
        db.connection.execute("CREATE TABLE notes (note TEXT)")
        db.connection.execute("INSERT INTO notes VALUES ('keep')")

    shapes_path.unlink()
    with ResultDatabase(f"sqlite:///{path}") as db:
        db.populate_shape_tables(VAV_DATA + 'ex:new a ex:Meter ; ex:label "fresh" .')
        assert db.connection.execute(
            "SELECT focus, label FROM [ex:MeterShape]"
        ).fetchall() == [("<https://example.org/new>", '"fresh"')]
        assert db.connection.execute("""
            SELECT focus, conforms FROM _shifty_result_validation
            WHERE table_name = 'ex:MeterShape'
        """).fetchall() == [("<https://example.org/new>", 1)]
        assert db.connection.execute("""
            SELECT missing FROM _shifty_result_binding_status
            WHERE table_name = 'ex:VavShape' AND field_name = 'airflow' ORDER BY focus
        """).fetchall() == [(1,), (0,)]
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
        db.add_query_table(
            "meters",
            "SELECT ?meter WHERE { ?meter a <https://example.org/MeterClass> }",
        )
        db.refresh(VAV_DATA + "ex:meter a ex:MeterClass .")
        db.connection.execute("""
            CREATE TRIGGER reject_refresh BEFORE INSERT ON meters BEGIN
                SELECT RAISE(ABORT, 'test load failure');
            END
        """)

        with pytest.raises(sqlite3.IntegrityError, match="test load failure"):
            db.populate_shape_tables(PREFIXES + "ex:new a ex:MeterClass .")

        assert db.connection.execute(
            "SELECT count(*) FROM [ex:VavShape]"
        ).fetchone() == (2,)
        assert db.connection.execute("SELECT * FROM [ex:Meter]").fetchall() == [
            ("<https://example.org/meter>",)
        ]
        assert db.connection.execute("SELECT * FROM meters").fetchall() == [
            ("<https://example.org/meter>",)
        ]
        assert db.connection.execute("""
            SELECT focus, conforms FROM _shifty_result_validation
            WHERE table_name = 'ex:Meter'
        """).fetchall() == [("<https://example.org/meter>", 1)]
        assert db.connection.execute("""
            SELECT missing FROM _shifty_result_binding_status
            WHERE table_name = 'ex:VavShape' AND field_name = 'airflow' ORDER BY focus
        """).fetchall() == [(1,), (0,)]


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
            'SELECT "select" FROM "Points" ORDER BY "select"'
        ).fetchall() == [
            ('"https://example.org/value"',),
            ("<https://example.org/value>",),
        ]
        assert db.connection.execute("""
            SELECT conforms FROM _shifty_result_validation WHERE table_name = 'Points'
        """).fetchall() == [(0,)]
        assert (
            db.connection.execute(
                "SELECT * FROM _shifty_result_missing_required_values"
            ).fetchall()
            == []
        )

        db.populate_shape_tables(PREFIXES + r'ex:focus ex:p "line\rbreak" .')
        assert db.connection.execute('SELECT "select" FROM "Points"').fetchall() == [
            (r'"line\rbreak"',)
        ]
        assert db.connection.execute("""
            SELECT conforms FROM _shifty_result_validation WHERE table_name = 'Points'
        """).fetchall() == [(1,)]


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
