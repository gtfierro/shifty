# SHACL Validator

A Shapes Constraint Language (SHACL) validator written in Rust. It provides both a command-line interface (CLI) for quick validation and a library for integration into other Rust applications.

This validator parses SHACL shapes and RDF data, executes the validation logic, and produces a standard SHACL validation report. It also includes powerful tools for visualizing shape graphs and debugging the validation process.

## Features

- **SHACL Core Validation**: Implements core SHACL constraints to validate RDF data against a set of shapes.
- **Multiple Report Formats**: Generates validation reports in Turtle (default), RDF/XML, and N-Triples.
- **Shape Graph Visualization**: Generates Graphviz DOT files to visualize the structure of your SHACL shapes, making them easier to understand and debug.
- **Performance Profiling**:
    - `heat` command: Shows which validation components are triggered most frequently, helping to identify performance bottlenecks.
    - `graphviz-heatmap`: Visualizes execution frequency directly on the shape graph, with hotter colors indicating more frequent execution.
- **Debugging Tools**: Provides detailed execution traces to understand the validation process step-by-step.

## Installation

To build and install the CLI from source, you will need Rust and Cargo installed.

1. Clone the repository.
2. Navigate to the project directory and run:


## Desired features
- add a 'data-graph' argument which takes an ontology name (from the local ontoenv) and uses `ontoenv.add` to fetch it into the ontology
- add configuration options for using the imports closure from ontoenv
