from shifty.experimental.result_database import ResultDatabase

db = ResultDatabase()
db.add_shape_tables("shapes.ttl")
db.add_query_table(
    "vav_locations",
    """
    PREFIX brick: <https://brickschema.org/schema/Brick#>
    PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
    PREFIX sh: <http://www.w3.org/ns/shacl#>

    SELECT ?focus ?profile_label ?room WHERE {
        ?focus a ?class .
        ?profile sh:targetClass ?class ; rdfs:label ?profile_label .
        OPTIONAL { ?focus brick:hasLocation ?room }
    }
    """,
)
db.refresh("data.ttl")

query = """
SELECT vav.focus, vav.airflow, locations.room, locations.profile_label
FROM [ex:VavShape] AS vav
JOIN vav_locations AS locations USING (focus)
ORDER BY vav.focus, vav.airflow
"""
for row in db.connection.execute(query):
    print(row)
