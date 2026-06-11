"""
Tests for SHACL feature coverage in shifty.

This module tests various SHACL constraint components:
- Datatype constraints (sh:datatype)
- Cardinality constraints (sh:minCount, sh:maxCount)
- NodeKind constraints (sh:nodeKind)
- Value set constraints (sh:in)
- Pattern constraints (sh:pattern)
- Closed shapes (sh:closed)
- Target types (sh:targetClass, sh:targetNode)
- Logical operators (sh:and, sh:or, sh:not)
- Nested property paths

Note: Some SHACL features may not be fully implemented yet.
"""

import pytest
import rdflib

import shifty

PREFIXES = """\
@prefix sh:  <http://www.w3.org/ns/shacl#> .
@prefix rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#> .
@prefix rdfs:<http://www.w3.org/2000/01/rdf-schema#> .
@prefix xsd: <http://www.w3.org/2001/XMLSchema#> .
@prefix ex:  <http://example.org/> .
"""

# ── datatype constraint ──────────────────────────────────────────────────────

class TestDatatypeConstraint:
    def test_valid_datatype_passes(self):
        shapes = PREFIXES + """\
        ex:PersonShape a sh:NodeShape ;
            sh:targetClass ex:Person ;
            sh:property [
                sh:path ex:age ;
                sh:datatype xsd:integer ;
            ] .
        """
        data = PREFIXES + "ex:Alice a ex:Person ; ex:age 30 ."
        result = shifty.validate_algebra(data.encode(), shapes.encode())
        assert result.conforms is True

    def test_invalid_datatype_fails(self):
        shapes = PREFIXES + """\
        ex:PersonShape a sh:NodeShape ;
            sh:targetClass ex:Person ;
            sh:property [
                sh:path ex:age ;
                sh:datatype xsd:integer ;
            ] .
        """
        data = PREFIXES + "ex:Alice a ex:Person ; ex:age \"thirty\" ."
        result = shifty.validate_algebra(data.encode(), shapes.encode())
        assert not result.conforms


# ── minCount/maxCount constraints ────────────────────────────────────────────

class TestMinMaxCount:
    def test_mincount_violation(self):
        shapes = PREFIXES + """\
        ex:PersonShape a sh:NodeShape ;
            sh:targetClass ex:Person ;
            sh:property [
                sh:path ex:name ;
                sh:minCount 2 ;
            ] .
        """
        data = PREFIXES + "ex:Alice a ex:Person ; ex:name \"Alice\" ."
        result = shifty.validate_algebra(data.encode(), shapes.encode())
        assert not result.conforms

    def test_maxcount_violation(self):
        shapes = PREFIXES + """\
        ex:PersonShape a sh:NodeShape ;
            sh:targetClass ex:Person ;
            sh:property [
                sh:path ex:age ;
                sh:maxCount 1 ;
            ] .
        """
        data = PREFIXES + "ex:Alice a ex:Person ; ex:age 30 ; ex:age 40 ."
        result = shifty.validate_algebra(data.encode(), shapes.encode())
        assert not result.conforms


# ── nodeKind constraint ──────────────────────────────────────────────────────

class TestNodeKind:
    def test_nodekind_literal(self):
        shapes = PREFIXES + """\
        ex:PersonShape a sh:NodeShape ;
            sh:targetClass ex:Person ;
            sh:property [
                sh:path ex:name ;
                sh:nodeKind sh:Literal ;
            ] .
        """
        data = PREFIXES + "ex:Alice a ex:Person ; ex:name \"Alice\" ."
        result = shifty.validate_algebra(data.encode(), shapes.encode())
        assert result.conforms

    def test_nodekind_uri_fails_for_literal(self):
        shapes = PREFIXES + """\
        ex:PersonShape a sh:NodeShape ;
            sh:targetClass ex:Person ;
            sh:property [
                sh:path ex:name ;
                sh:nodeKind sh:Literal ;
            ] .
        """
        data = PREFIXES + "ex:Alice a ex:Person ; ex:name ex:NameNode ."
        result = shifty.validate_algebra(data.encode(), shapes.encode())
        assert not result.conforms


# ── in/notIn constraints ─────────────────────────────────────────────────────

class TestInNotIn:
    def test_in_constraint_passes(self):
        shapes = PREFIXES + """\
        ex:PersonShape a sh:NodeShape ;
            sh:targetClass ex:Person ;
            sh:property [
                sh:path ex:status ;
                sh:in ( ex:Active ex:Inactive ) ;
            ] .
        """
        data = PREFIXES + "ex:Alice a ex:Person ; ex:status ex:Active ."
        result = shifty.validate_algebra(data.encode(), shapes.encode())
        assert result.conforms

    def test_in_constraint_fails(self):
        shapes = PREFIXES + """\
        ex:PersonShape a sh:NodeShape ;
            sh:targetClass ex:Person ;
            sh:property [
                sh:path ex:status ;
                sh:in ( ex:Active ex:Inactive ) ;
            ] .
        """
        data = PREFIXES + "ex:Alice a ex:Person ; ex:status ex:Pending ."
        result = shifty.validate_algebra(data.encode(), shapes.encode())
        assert not result.conforms


# ── pattern constraint ───────────────────────────────────────────────────────

class TestPattern:
    def test_pattern_passes(self):
        shapes = PREFIXES + """\
        ex:PersonShape a sh:NodeShape ;
            sh:targetClass ex:Person ;
            sh:property [
                sh:path ex:email ;
                sh:pattern \"^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\\\\.[a-zA-Z]{2,}$\" ;
            ] .
        """
        data = PREFIXES + 'ex:Alice a ex:Person ; ex:email "alice@example.com" .'
        result = shifty.validate_algebra(data.encode(), shapes.encode())
        assert result.conforms

    def test_pattern_fails(self):
        shapes = PREFIXES + """\
        ex:PersonShape a sh:NodeShape ;
            sh:targetClass ex:Person ;
            sh:property [
                sh:path ex:email ;
                sh:pattern \"^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\\\\.[a-zA-Z]{2,}$\" ;
            ] .
        """
        data = PREFIXES + 'ex:Alice a ex:Person ; ex:email "not-an-email" .'
        result = shifty.validate_algebra(data.encode(), shapes.encode())
        assert not result.conforms


# ── closed constraint (partial implementation) ───────────────────────────────

class TestClosed:
    def test_closed_fails_with_extra_property(self):
        shapes = PREFIXES + """\
        ex:PersonShape a sh:NodeShape ;
            sh:targetClass ex:Person ;
            sh:property [
                sh:path ex:name ;
            ] ;
            sh:closed true .
        """
        data = PREFIXES + "ex:Alice a ex:Person ; ex:name \"Alice\" ; ex:extra \"Property\" ."
        result = shifty.validate_algebra(data.encode(), shapes.encode())
        assert not result.conforms


# ── targetClass vs targetNode ────────────────────────────────────────────────

class TestTargetTypes:
    def test_target_class(self):
        shapes = PREFIXES + """\
        ex:PersonShape a sh:NodeShape ;
            sh:targetClass ex:Person ;
            sh:property [
                sh:path ex:name ;
                sh:minCount 1 ;
            ] .
        """
        data = PREFIXES + "ex:Alice a ex:Person ; ex:name \"Alice\" ."
        result = shifty.validate_algebra(data.encode(), shapes.encode())
        assert result.conforms

    def test_target_node(self):
        shapes = PREFIXES + """\
        ex:PersonShape a sh:NodeShape ;
            sh:targetNode ex:Alice ;
            sh:property [
                sh:path ex:name ;
                sh:minCount 1 ;
            ] .
        """
        data = PREFIXES + "ex:Alice a ex:Person ; ex:name \"Alice\" ."
        result = shifty.validate_algebra(data.encode(), shapes.encode())
        assert result.conforms


# ── sh:and, sh:or, sh:not ────────────────────────────────────────────────────

class TestLogicalOperators:
    def test_sh_and(self):
        shapes = PREFIXES + """\
        ex:PersonShape a sh:NodeShape ;
            sh:targetClass ex:Person ;
            sh:and (
                [ sh:property [ sh:path ex:name ; sh:minCount 1 ] ]
                [ sh:property [ sh:path ex:age ; sh:maxCount 1 ] ]
            ) .
        """
        data = PREFIXES + "ex:Alice a ex:Person ; ex:name \"Alice\" ; ex:age 30 ."
        result = shifty.validate_algebra(data.encode(), shapes.encode())
        assert result.conforms

    def test_sh_and_fails(self):
        shapes = PREFIXES + """\
        ex:PersonShape a sh:NodeShape ;
            sh:targetClass ex:Person ;
            sh:and (
                [ sh:property [ sh:path ex:name ; sh:minCount 1 ] ]
                [ sh:property [ sh:path ex:age ; sh:maxCount 1 ] ]
            ) .
        """
        data = PREFIXES + "ex:Alice a ex:Person ; ex:name \"Alice\" ; ex:age 30 ; ex:age 40 ."
        result = shifty.validate_algebra(data.encode(), shapes.encode())
        assert not result.conforms


# ── Nested property paths ────────────────────────────────────────────────────

class TestNestedPaths:
    def test_nested_path(self):
        shapes = PREFIXES + """\
        ex:AddressShape a sh:NodeShape ;
            sh:property [
                sh:path ex:street ;
                sh:minCount 1 ;
            ] .
        ex:PersonShape a sh:NodeShape ;
            sh:targetClass ex:Person ;
            sh:property [
                sh:path ( ex:address ) ;
                sh:node ex:AddressShape ;
            ] .
        """
        data = PREFIXES + "ex:Alice a ex:Person ; ex:address [ ex:street \"Main St\" ] ."
        result = shifty.validate_algebra(data.encode(), shapes.encode())
        assert result.conforms
