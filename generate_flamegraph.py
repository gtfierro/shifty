#!/usr/bin/env python3
import argparse
import json
import re
import sys
from collections import defaultdict
from typing import Optional

SOURCE_PATTERN = re.compile(r"(\w+)\((\w+)\((\d+)\)\)")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Convert shifty trace JSONL into folded stacks for inferno-flamegraph."
    )
    parser.add_argument("input_file", help="Path to trace JSONL file")
    parser.add_argument(
        "--shape-source",
        choices=("shape-execution", "typed-shapes"),
        default="shape-execution",
        help=(
            "Which shape events to convert into stack frames. "
            "Use shape-execution to avoid double-counting when both generic and typed "
            "shape events are present."
        ),
    )
    parser.add_argument(
        "--include-rules",
        action="store_true",
        help="Include SHACL rule execution frames in the folded output.",
    )
    return parser.parse_args()


def load_events(input_file: str) -> list[dict]:
    events = []
    with open(input_file, "r", encoding="utf-8") as handle:
        for line in handle:
            line = line.strip()
            if not line:
                continue
            try:
                events.append(json.loads(line))
            except json.JSONDecodeError:
                continue
    return events


def source_to_frame(raw_source: str) -> str:
    match = SOURCE_PATTERN.fullmatch(raw_source)
    if not match:
        return raw_source
    shape_type, _id_type, id_value = match.groups()
    return f"{shape_type}_{id_value}"


def pop_frame(stack: list[str], expected: Optional[str] = None) -> None:
    if not stack:
        return
    if expected is None or stack[-1] == expected:
        stack.pop()
        return

    for index in range(len(stack) - 1, -1, -1):
        if stack[index] == expected:
            del stack[index:]
            return

    stack.pop()


def generate_flamegraph(input_file: str, shape_source: str, include_rules: bool) -> None:
    events = load_events(input_file)
    current_stack: list[str] = []
    folded: defaultdict[str, int] = defaultdict(int)
    last_ts: Optional[int] = None

    for event in events:
        event_type = event.get("type")
        ts = event.get("ts")
        if event_type is None or ts is None:
            continue

        if last_ts is not None and current_stack:
            duration = ts - last_ts
            if duration > 0:
                folded[";".join(current_stack)] += duration
        last_ts = ts

        if shape_source == "shape-execution":
            if event_type == "EnterShapeExecution":
                current_stack.append(source_to_frame(event["source"]))
            elif event_type == "ExitShapeExecution":
                pop_frame(current_stack, source_to_frame(event["source"]))
        else:
            if event_type == "EnterNodeShape":
                current_stack.append(f"NodeShape_{event['node_shape_id']}")
            elif event_type == "ExitNodeShape":
                pop_frame(current_stack, f"NodeShape_{event['node_shape_id']}")
            elif event_type == "EnterPropertyShape":
                current_stack.append(f"PropertyShape_{event['property_shape_id']}")
            elif event_type == "ExitPropertyShape":
                pop_frame(current_stack, f"PropertyShape_{event['property_shape_id']}")

        if include_rules:
            if event_type == "EnterRule":
                current_stack.append(f"Rule_{event['rule_id']}")
            elif event_type == "ExitRule":
                pop_frame(current_stack, f"Rule_{event['rule_id']}")

    for stack, duration in sorted(folded.items()):
        if duration > 0:
            print(f"{stack} {duration}")


if __name__ == "__main__":
    args = parse_args()
    generate_flamegraph(args.input_file, args.shape_source, args.include_rules)
