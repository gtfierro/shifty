from shifty.experimental.result_database import ResultDatabase

db = ResultDatabase()
db.add_shape_tables("shapes.ttl")
db.refresh("data.ttl")

query = """
SELECT result_id, focus, conforms
FROM _shifty_result_validation
ORDER BY focus
"""
for row in db.connection.execute(query):
    print(row)
