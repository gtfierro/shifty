from shifty.experimental.result_database import ResultDatabase

db = ResultDatabase()
db.add_shape_tables("shapes.ttl")
db.refresh("data.ttl")

query = """
SELECT * FROM [ex:VavShape] ORDER BY focus, airflow
"""
for row in db.connection.execute(query):
    print(row)
