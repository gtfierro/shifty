"""Query full and partial VAV results in an in-memory SQLite database.

Run from ``python/`` with ``uv run python examples/result_database.py``.
"""

import sqlite3

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
    with ResultDatabase() as db:
        db.add_shape_tables(SHAPES)
        db.populate_shape_tables(DATA, infer=False)

        # Join the repeatable airflow field and its status on the focus key.
        rows = db.connection.execute("""
            SELECT v.focus, v.conforms, v.supply_air_temperature, a.value, s.missing
            FROM [ex:VavShape] AS v
            LEFT JOIN [ex:VavShape_airflow] AS a ON a.focus = v.focus
            JOIN [ex:VavShape_slot_status] AS s
                ON s.focus = v.focus AND s.field = 'airflow'
            ORDER BY v.focus, a.value
        """)
        for focus, conforms, temperature, airflow, missing in rows:
            status = "conforms" if conforms else "partial"
            print(f"{focus} ({status})")
            print(f"  Supply air temperature  {temperature}")
            print(f"  Airflow  {airflow or f'missing {missing}'}")

        try:
            db.connection.execute(
                "INSERT INTO [ex:VavShape_airflow] (focus, value) VALUES (?, ?)",
                ("<https://example.org/unknown-vav>", "<https://example.org/flow2>"),
            )
        except sqlite3.IntegrityError:
            print("\nSQLite rejected an airflow row with no matching VAV focus.")
        else:
            raise RuntimeError("Expected SQLite to enforce the airflow foreign key")


if __name__ == "__main__":
    main()
