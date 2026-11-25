"""Query full and partial VAV results in a new SQLite database.

Run from ``python/`` with ``uv run python examples/result_database.py``.
"""

import argparse
from pathlib import Path

from shifty.experimental.result_database import ResultDatabase

SHAPES = """
@prefix ex: <https://example.org/> .
@prefix sh: <http://www.w3.org/ns/shacl#> .

ex:VavShape a sh:NodeShape ;
    sh:targetClass ex:Vav ;
    sh:property [
        sh:path ex:supplyAirTemperature ;
        sh:minCount 1 ;
        sh:maxCount 1
    ], [
        sh:path ex:airflow ;
        sh:minCount 1
    ] .
"""

DATA = """
@prefix ex: <https://example.org/> .

ex:vav1 a ex:Vav ; ex:supplyAirTemperature ex:sat1 .
ex:vav2 a ex:Vav ; ex:supplyAirTemperature ex:sat2 ; ex:airflow ex:flow2, ex:flow3 .
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
        db.populate_shape_tables(DATA, infer=False)

        # Each airflow sensor gets a row. A VAV with no sensor still appears.
        rows = db.connection.execute("""
            SELECT focus, supply_air_temperature, airflow
            FROM [ex:VavShape]
            ORDER BY focus, airflow
        """)
        print("focus | supply_air_temperature | airflow")
        for focus, temperature, airflow in rows:
            print(f"{focus} | {temperature or 'NULL'} | {airflow or 'NULL'}")


if __name__ == "__main__":
    main()
