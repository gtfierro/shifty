#!/usr/bin/env python3
import argparse
import json
from collections import defaultdict
from pathlib import Path
from typing import Optional
import html
import re


def format_ns_as_ms(ns: int) -> str:
    return f"{ns / 1_000_000:.3f} ms"


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

def frame_name(event: dict, fallback: str) -> str:
    frame = event.get("frame")
    if isinstance(frame, str) and frame:
        return frame
    return fallback


def load_frame_metadata(input_file: str) -> dict[str, dict]:
    metadata_path = input_file.removesuffix(".jsonl") + ".shapes.json"
    try:
        with open(metadata_path, "r", encoding="utf-8") as handle:
            payload = json.load(handle)
    except FileNotFoundError:
        return {}
    shapes = payload.get("frames", payload.get("shapes", []))
    return {
        item["frame"]: item
        for item in shapes
        if isinstance(item, dict) and isinstance(item.get("frame"), str)
    }


def compute_frame_stats(
    input_file: str, shape_source: str, include_rules: bool
) -> tuple[dict[str, dict[str, int]], int]:
    events = load_events(input_file)
    active_stack: list[dict[str, int | str]] = []
    stats: dict[str, dict[str, int]] = defaultdict(
        lambda: {
            "count": 0,
            "self_ns": 0,
            "inclusive_ns": 0,
        }
    )
    total_traced_ns = 0

    def close_frame(expected_frame: Optional[str], ts: int) -> None:
        nonlocal total_traced_ns
        if not active_stack:
            return

        match_index: Optional[int] = None
        if expected_frame is None:
            match_index = len(active_stack) - 1
        else:
            for index in range(len(active_stack) - 1, -1, -1):
                if active_stack[index]["frame"] == expected_frame:
                    match_index = index
                    break

        if match_index is None:
            return

        while len(active_stack) - 1 > match_index:
            orphan = active_stack.pop()
            total_duration = max(0, ts - int(orphan["start_ts"]))
            self_duration = max(0, total_duration - int(orphan["child_time"]))
            orphan_stats = stats[str(orphan["frame"])]
            orphan_stats["count"] += 1
            orphan_stats["self_ns"] += self_duration
            orphan_stats["inclusive_ns"] += total_duration
            total_traced_ns += total_duration
            if active_stack:
                active_stack[-1]["child_time"] += total_duration

        frame = active_stack.pop()
        total_duration = max(0, ts - int(frame["start_ts"]))
        self_duration = max(0, total_duration - int(frame["child_time"]))
        frame_stats = stats[str(frame["frame"])]
        frame_stats["count"] += 1
        frame_stats["self_ns"] += self_duration
        frame_stats["inclusive_ns"] += total_duration
        total_traced_ns += total_duration
        if active_stack:
            active_stack[-1]["child_time"] += total_duration

    for event in events:
        ts = event.get("ts")
        if ts is None:
            continue

        frame = event_frame(event, shape_source, include_rules)
        if frame is None:
            continue

        if is_enter_event(event, shape_source, include_rules):
            active_stack.append(
                {
                    "frame": frame,
                    "start_ts": ts,
                    "child_time": 0,
                }
            )
        elif is_exit_event(event, shape_source, include_rules):
            close_frame(frame, ts)

    return stats, total_traced_ns


def inject_tooltips(
    input_file: str, svg_path: str, shape_source: str, include_rules: bool
) -> None:
    metadata = load_frame_metadata(input_file)
    stats, total_traced_ns = compute_frame_stats(input_file, shape_source, include_rules)
    if not metadata and not stats:
        return
    svg_file = Path(svg_path)
    content = svg_file.read_text(encoding="utf-8")

    def replace_title(match: re.Match[str]) -> str:
        title_text = match.group(1)
        if title_text == "all" or title_text.startswith("all ("):
            return match.group(0)
        frame, sep, suffix = title_text.partition(" (")
        item = metadata.get(frame)
        tooltip = frame
        if sep:
            tooltip += f" ({suffix}"
        frame_stats = stats.get(frame)
        if frame_stats:
            inclusive_ns = frame_stats["inclusive_ns"]
            self_ns = frame_stats["self_ns"]
            tooltip += "\nCount: " + str(frame_stats["count"])
            tooltip += "\nInclusive: " + format_ns_as_ms(inclusive_ns)
            tooltip += "\nSelf: " + format_ns_as_ms(self_ns)
            if total_traced_ns > 0:
                tooltip += f"\nTrace Share: {(inclusive_ns / total_traced_ns) * 100:.2f}%"
        if item is None:
            return f"<title>{html.escape(tooltip)}</title>"
        labels = [label for label in item.get("labels", []) if isinstance(label, str) and label]
        messages = [
            message
            for message in item.get("messages", [])
            if isinstance(message, str) and message
        ]
        args = [arg for arg in item.get("args", []) if isinstance(arg, str) and arg]
        component_kind = item.get("component_kind")
        if isinstance(component_kind, str) and component_kind:
            tooltip += "\nComponent: " + component_kind
        if args:
            tooltip += "\nArgs: " + " | ".join(args)
        if labels:
            tooltip += "\nLabel: " + " | ".join(labels)
        if messages:
            tooltip += "\nMessage: " + " | ".join(messages)
        return f"<title>{html.escape(tooltip)}</title>"

    updated = re.sub(r"<title>(.*?)</title>", replace_title, content, flags=re.DOTALL)
    svg_file.write_text(updated, encoding="utf-8")


def event_frame(event: dict, shape_source: str, include_rules: bool) -> Optional[str]:
    event_type = event.get("type")
    if shape_source == "shape-execution":
        if event_type in ("EnterShapeExecution", "ExitShapeExecution"):
            return frame_name(event, str(event.get("source", "shape")))
    else:
        if event_type in ("EnterNodeShape", "ExitNodeShape"):
            return frame_name(event, f"NodeShape_{event['node_shape_id']}")
        if event_type in ("EnterPropertyShape", "ExitPropertyShape"):
            return frame_name(event, f"PropertyShape_{event['property_shape_id']}")

    if event_type in ("EnterComponent", "ExitComponent"):
        return frame_name(event, f"Component_{event['component_id']}")

    if include_rules and event_type in ("EnterRule", "ExitRule"):
        return frame_name(event, f"Rule_{event['rule_id']}")

    return None


def build_folded_stacks(
    input_file: str, shape_source: str, include_rules: bool
) -> defaultdict[str, int]:
    events = load_events(input_file)
    active_stack: list[dict[str, int | str]] = []
    folded: defaultdict[str, int] = defaultdict(int)

    for event in events:
        ts = event.get("ts")
        if ts is None:
            continue

        frame = event_frame(event, shape_source, include_rules)
        if frame is None:
            continue

        if is_enter_event(event, shape_source, include_rules):
            active_stack.append(
                {
                    "frame": frame,
                    "start_ts": ts,
                    "child_time": 0,
                }
            )
        elif is_exit_event(event, shape_source, include_rules):
            close_frame(active_stack, folded, frame, ts)

    return folded


def is_enter_event(event: dict, shape_source: str, include_rules: bool) -> bool:
    event_type = event.get("type")
    if event_type == "EnterShapeExecution" and shape_source == "shape-execution":
        return True
    if event_type in ("EnterNodeShape", "EnterPropertyShape") and shape_source != "shape-execution":
        return True
    if event_type == "EnterComponent":
        return True
    if include_rules and event_type == "EnterRule":
        return True
    return False


def is_exit_event(event: dict, shape_source: str, include_rules: bool) -> bool:
    event_type = event.get("type")
    if event_type == "ExitShapeExecution" and shape_source == "shape-execution":
        return True
    if event_type in ("ExitNodeShape", "ExitPropertyShape") and shape_source != "shape-execution":
        return True
    if event_type == "ExitComponent":
        return True
    if include_rules and event_type == "ExitRule":
        return True
    return False


def close_frame(
    active_stack: list[dict[str, int | str]],
    folded: defaultdict[str, int],
    expected_frame: Optional[str],
    ts: int,
) -> None:
    if not active_stack:
        return

    match_index: Optional[int] = None
    if expected_frame is None:
        match_index = len(active_stack) - 1
    else:
        for index in range(len(active_stack) - 1, -1, -1):
            if active_stack[index]["frame"] == expected_frame:
                match_index = index
                break

    if match_index is None:
        return

    while len(active_stack) - 1 > match_index:
        orphan = active_stack.pop()
        parent_path = [str(item["frame"]) for item in active_stack]
        total_duration = max(0, ts - int(orphan["start_ts"]))
        self_duration = max(0, total_duration - int(orphan["child_time"]))
        if self_duration > 0:
            folded[";".join(parent_path + [str(orphan["frame"])])] += self_duration
        if active_stack:
            active_stack[-1]["child_time"] += total_duration

    frame = active_stack.pop()
    parent_path = [str(item["frame"]) for item in active_stack]
    total_duration = max(0, ts - int(frame["start_ts"]))
    self_duration = max(0, total_duration - int(frame["child_time"]))
    if self_duration > 0:
        folded[";".join(parent_path + [str(frame["frame"])])] += self_duration
    if active_stack:
        active_stack[-1]["child_time"] += total_duration


def generate_flamegraph(input_file: str, shape_source: str, include_rules: bool) -> None:
    folded = build_folded_stacks(input_file, shape_source, include_rules)
    for stack, duration in sorted(folded.items()):
        if duration > 0:
            print(f"{stack} {duration}")


if __name__ == "__main__":
    args = parse_args()
    if args.inject_tooltips:
        inject_tooltips(
            args.input_file,
            args.inject_tooltips,
            args.shape_source,
            args.include_rules,
        )
    else:
        generate_flamegraph(args.input_file, args.shape_source, args.include_rules)
