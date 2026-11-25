"""Query full and partial VAV results in a new SQLite database.

Run from ``python/`` with ``uv run python examples/result_database.py``.
"""

import argparse
import sqlite3
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
ex:vav2 a ex:Vav ; ex:supplyAirTemperature ex:sat2 ; ex:airflow ex:flow2 .
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

        # Repeatable airflow values appear as a JSON array. The status explains missing airflow.
        rows = db.connection.execute("""
            SELECT v.focus, v.conforms, v.supply_air_temperature, v.airflow, s.missing
            FROM [ex:VavShape] AS v
            JOIN [ex:VavShape_slot_status] AS s
                ON s.focus = v.focus AND s.field = 'airflow'
            ORDER BY v.focus
        """)
        for focus, conforms, temperature, airflow, missing in rows:
            status = "conforms" if conforms else "partial"
            print(f"{focus} ({status})")
            print(f"  Supply air temperature  {temperature}")
            print(f"  Airflow  {airflow or f'missing {missing}'}")

        try:
            db.connection.execute(
                "INSERT INTO [ex:VavShape_slot_status] (focus, field, status) "
                "VALUES (?, 'airflow', 'pass')",
                ("<https://example.org/unknown-vav>",),
            )
        except sqlite3.IntegrityError:
            print("\nSQLite rejected a status row with no matching VAV focus.")
        else:
            raise RuntimeError("Expected SQLite to enforce the status foreign key")


if __name__ == "__main__":
    main()
