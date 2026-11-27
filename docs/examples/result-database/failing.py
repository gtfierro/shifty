from shifty.experimental.result_database import ResultDatabase

db = ResultDatabase()
db.add_shape_tables("shapes.ttl")
db.refresh("data.ttl")

query = """
SELECT vav.*
FROM [ex:VavShape] AS vav
JOIN _shifty_result_validation USING (result_id)
WHERE conforms = 0
"""
for row in db.connection.execute(query):
    print(row)
