"""Create SHACL and SPARQL result tables for VAVs in a new SQLite database.

Run from ``python/`` with ``uv run python examples/result_database.py``.
"""

import argparse
from pathlib import Path

from shifty.experimental.result_database import ResultDatabase

SHAPES = """
@prefix brick: <https://brickschema.org/schema/Brick#> .
@prefix ex: <https://example.org/> .
@prefix rdfs: <http://www.w3.org/2000/01/rdf-schema#> .
@prefix sh: <http://www.w3.org/ns/shacl#> .

ex:VavShape a sh:NodeShape ;
    rdfs:label "VAV commissioning profile" ;
    sh:targetClass brick:VAV ;
    sh:property [
        rdfs:label "supply air temperature" ;
        sh:path brick:hasPoint ;
        sh:qualifiedValueShape [ sh:class brick:Supply_Air_Temperature_Sensor ] ;
        sh:qualifiedMinCount 1 ;
        sh:qualifiedMaxCount 1
    ], [
        rdfs:label "airflow" ;
        sh:path brick:hasPoint ;
        sh:qualifiedValueShape [ sh:class brick:Air_Flow_Sensor ] ;
        sh:qualifiedMinCount 1
    ] .
"""

DATA = """
@prefix brick: <https://brickschema.org/schema/Brick#> .
@prefix ex: <https://example.org/> .

ex:vav1 a brick:VAV ; brick:hasPoint ex:sat1 .
ex:vav2 a brick:VAV ;
    brick:hasPoint ex:sat2, ex:flow2, ex:flow3 ;
    brick:hasLocation ex:room101 .

ex:sat1 a brick:Supply_Air_Temperature_Sensor .
ex:sat2 a brick:Supply_Air_Temperature_Sensor .
ex:flow2 a brick:Air_Flow_Sensor .
ex:flow3 a brick:Air_Flow_Sensor .
ex:room101 a brick:Room .
"""

VAV_LOCATIONS = """
PREFIX brick: <https://brickschema.org/schema/Brick#>
PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
PREFIX sh: <http://www.w3.org/ns/shacl#>

SELECT ?focus ?profile_label ?room WHERE {
    ?focus a ?class .
    ?profile sh:targetClass ?class ; rdfs:label ?profile_label .
    OPTIONAL { ?focus brick:hasLocation ?room }
}
"""


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "database",
        nargs="?",
        type=Path,
        default=Path("result.db"),
        help="new database file (default: %(default)s)",
    )
    database = parser.parse_args().database
    try:
        database.touch(exist_ok=False)
    except FileExistsError:
        parser.error(f"{database} already exists. Choose a new output file.")

    with ResultDatabase(database) as db:
        db.add_shape_tables(SHAPES)
        db.add_query_table("vav_locations", VAV_LOCATIONS)
        db.refresh(DATA, infer=False)

        # Each airflow sensor gets a row. A VAV with no sensor still appears.
        rows = db.connection.execute("""
            SELECT focus, supply_air_temperature, airflow
            FROM [ex:VavShape]
            ORDER BY focus, airflow
        """)
        print("ex:VavShape\nfocus | supply_air_temperature | airflow")
        for focus, temperature, airflow in rows:
            print(f"{focus} | {temperature or 'NULL'} | {airflow or 'NULL'}")

        print("\nvav_locations\nfocus | profile_label | room")
        for focus, profile_label, room in db.connection.execute(
            "SELECT focus, profile_label, room FROM vav_locations ORDER BY focus"
        ):
            print(f"{focus} | {profile_label} | {room or 'NULL'}")


if __name__ == "__main__":
    main()
