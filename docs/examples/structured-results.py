from pathlib import Path

import shifty


example_dir = Path(__file__).parent / "quick-start"
result = shifty.validate_algebra(
    example_dir / "data.ttl",
    example_dir / "shapes.ttl",
)

print("conforms:", result.conforms)
print("violations:", len(result.violations))
for violation in result.violations:
    print(violation.focus_node, violation.severity)
    for reason in violation.reasons:
        print("  ", reason.constraint_kind, reason.path, reason.value)
        print("  ", reason.message)
