"""Packaging contract tests for the native extension's type declarations."""

import ast
from pathlib import Path

import shifty._shifty as native


def test_native_stub_covers_runtime_exports() -> None:
    stub = Path(native.__file__).with_name("_shifty.pyi")
    assert stub.is_file()

    tree = ast.parse(stub.read_text())
    declared = {
        node.name
        for node in tree.body
        if isinstance(node, (ast.ClassDef, ast.FunctionDef, ast.AsyncFunctionDef))
    }
    runtime = {name for name in dir(native) if not name.startswith("__")}

    assert runtime <= declared


def test_native_stub_covers_public_class_members() -> None:
    stub = Path(native.__file__).with_name("_shifty.pyi")
    tree = ast.parse(stub.read_text())

    declared = {}
    for node in tree.body:
        if not isinstance(node, ast.ClassDef):
            continue
        members = {
            item.name
            for item in node.body
            if isinstance(item, (ast.FunctionDef, ast.AsyncFunctionDef))
        }
        members.update(
            item.target.id
            for item in node.body
            if isinstance(item, ast.AnnAssign) and isinstance(item.target, ast.Name)
        )
        declared[node.name] = members

    for name, members in declared.items():
        runtime_class = getattr(native, name)
        runtime_members = {
            member for member in vars(runtime_class) if not member.startswith("_")
        }
        assert runtime_members <= members, name
