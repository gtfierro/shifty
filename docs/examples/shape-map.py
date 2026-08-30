import shifty


shapes = """
@prefix ex: <http://example.org/> .
@prefix sh: <http://www.w3.org/ns/shacl#> .

ex:VavShape a sh:NodeShape ;
    sh:targetClass ex:Vav ;
    sh:property [
        sh:path ex:hasPoint ;
        sh:name "supply air temperature" ;
        sh:qualifiedValueShape [ sh:class ex:SupplyAirTemperatureSensor ] ;
        sh:qualifiedMinCount 1 ] ;
    sh:property [
        sh:path ex:hasPoint ;
        sh:name "airflow" ;
        sh:qualifiedValueShape [ sh:class ex:AirFlowSensor ] ;
        sh:qualifiedMinCount 1 ] .
"""

data = """
@prefix ex: <http://example.org/> .
ex:vav-1 a ex:Vav ; ex:hasPoint ex:sat-1 .
ex:sat-1 a ex:SupplyAirTemperatureSensor .
ex:vav-2 a ex:Vav ; ex:hasPoint ex:sat-2, ex:flow-2 .
ex:sat-2 a ex:SupplyAirTemperatureSensor .
ex:flow-2 a ex:AirFlowSensor .
"""

smap = shifty.shape_map(data, shapes, infer=False)

for mapping in smap["http://example.org/VavShape"]:
    print(mapping.focus, "conforms:", mapping.conforms)
    for key, binding in mapping.successful:
        print("   ", binding.name, binding.values)
