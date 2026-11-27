from shifty.experimental.result_database import ResultDatabase

db = ResultDatabase("building.sqlite")
db.add_shape_tables("shapes.ttl")
db.refresh("data.ttl")
db.close()
