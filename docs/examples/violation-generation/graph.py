"""The graph and shapes for the violation-generation tutorial."""

SHAPES = """
@prefix sh: <http://www.w3.org/ns/shacl#> .
@prefix xsd: <http://www.w3.org/2001/XMLSchema#> .
@prefix ex: <http://ex/> .

ex:SensorShape a sh:NodeShape ;
    sh:targetClass ex:Sensor ;
    sh:property ex:label, ex:unit, ex:room .

ex:label sh:path ex:label ; sh:datatype xsd:string ; sh:minCount 1 ; sh:maxCount 1 .
ex:unit sh:path ex:unit ; sh:in ( "degC" "degF" ) .
ex:room sh:path ex:inRoom ; sh:class ex:Room ; sh:minCount 1 .
"""

DATA = """
@prefix ex: <http://ex/> .

ex:s1 a ex:Sensor ; ex:label "Temp 1" ; ex:unit "degC" ; ex:inRoom ex:r1 .
ex:s2 a ex:Sensor ; ex:label "Temp 2" ; ex:unit "degF" ; ex:inRoom ex:r2 .
ex:r1 a ex:Room .
ex:r2 a ex:Room .
"""
