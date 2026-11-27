from shifty.experimental.result_database import ResultDatabase

db = ResultDatabase()
db.add_shape_tables("shapes.ttl")
db.refresh("data.ttl")

query = """
SELECT focus, field_name, missing
FROM _shifty_result_missing_required_values
"""
for row in db.connection.execute(query):
    print(row)
