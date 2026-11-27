from shifty.experimental.result_database import ResultDatabase

db = ResultDatabase()
db.add_shape_tables("shapes.ttl")
db.refresh("data.ttl")

query = """
SELECT focus, field_name, status, value_count, missing
FROM _shifty_result_binding_status
ORDER BY focus, field_name
"""
for row in db.connection.execute(query):
    print(row)
