#!/usr/bin/env python3
import argparse
import json
from collections import defaultdict
from pathlib import Path
from typing import Optional
import html
import re


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
            "Use 'shape-execution' to avoid double-counting when both generic and typed "
            "shape events are present."
        ),
    )
    parser.add_argument(
        "--include-rules",
        action="store_true",
        help="Include SHACL rule execution frames in the folded output.",
    )
    parser.add_argument(
        "--inject-tooltips",
        metavar="SVG",
        help="Patch an inferno flamegraph SVG in place using <trace>.shapes.json metadata.",
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


def frame_name(event: dict, fallback: str) -> str:
    frame = event.get("frame")
    if isinstance(frame, str) and frame:
        return frame
    return fallback




def load_shape_metadata(input_file: str) -> dict[str, dict]:
    metadata_path = input_file.removesuffix(".jsonl") + ".shapes.json"
    try:
        with open(metadata_path, "r", encoding="utf-8") as handle:
            payload = json.load(handle)
    except FileNotFoundError:
        return {}
    shapes = payload.get("shapes", [])
    return {
        item["frame"]: item
        for item in shapes
        if isinstance(item, dict) and isinstance(item.get("frame"), str)
    }


def inject_tooltips(input_file: str, svg_path: str) -> None:
    metadata = load_shape_metadata(input_file)
    if not metadata:
        return
    svg_file = Path(svg_path)
    content = svg_file.read_text(encoding="utf-8")

    def replace_title(match: re.Match[str]) -> str:
        title_text = match.group(1)
        if title_text == "all" or title_text.startswith("all ("):
            return match.group(0)
        frame, sep, suffix = title_text.partition(" (")
        shape = metadata.get(frame)
        if shape is None:
            return match.group(0)
        tooltip = frame
        if sep:
            tooltip += f" ({suffix}"
        labels = [label for label in shape.get("labels", []) if isinstance(label, str) and label]
        messages = [
            message
            for message in shape.get("messages", [])
            if isinstance(message, str) and message
        ]
        if labels:
            tooltip += "\nLabel: " + " | ".join(labels)
        if messages:
            tooltip += "\nMessage: " + " | ".join(messages)
        return f"<title>{html.escape(tooltip)}</title>"

    updated = re.sub(r"<title>(.*?)</title>", replace_title, content, flags=re.DOTALL)
    svg_file.write_text(updated, encoding="utf-8")


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
                current_stack.append(frame_name(event, str(event.get("source", "shape"))))
            elif event_type == "ExitShapeExecution":
                pop_frame(current_stack, frame_name(event, str(event.get("source", "shape"))))
        else:
            if event_type == "EnterNodeShape":
                current_stack.append(frame_name(event, f"NodeShape_{event['node_shape_id']}"))
            elif event_type == "ExitNodeShape":
                pop_frame(current_stack, frame_name(event, f"NodeShape_{event['node_shape_id']}"))
            elif event_type == "EnterPropertyShape":
                current_stack.append(
                    frame_name(event, f"PropertyShape_{event['property_shape_id']}")
                )
            elif event_type == "ExitPropertyShape":
                pop_frame(
                    current_stack,
                    frame_name(event, f"PropertyShape_{event['property_shape_id']}"),
                )

        if include_rules:
            if event_type == "EnterRule":
                current_stack.append(frame_name(event, f"Rule_{event['rule_id']}"))
            elif event_type == "ExitRule":
                pop_frame(current_stack, frame_name(event, f"Rule_{event['rule_id']}"))

    for stack, duration in sorted(folded.items()):
        if duration > 0:
            print(f"{stack} {duration}")


if __name__ == "__main__":
    args = parse_args()
    if args.inject_tooltips:
        inject_tooltips(args.input_file, args.inject_tooltips)
    else:
        generate_flamegraph(args.input_file, args.shape_source, args.include_rules)
