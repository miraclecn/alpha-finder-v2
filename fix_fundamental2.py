"""Fix remaining path issues in test_fundamental_research_input_builder.py."""
content = open("tests/test_fundamental_research_input_builder.py", encoding="utf-8").read()
original = content

# Fix {sleeve_path}" → {sleeve_path.as_posix()}" (sleeve_path is a Path object)
content = content.replace('{sleeve_path}"', '{sleeve_path.as_posix()}"')

# Fix {residual_snapshot_path}" → {residual_snapshot_path.as_posix()}"
content = content.replace('{residual_snapshot_path}"', '{residual_snapshot_path.as_posix()}"')

# Fix f'output_path = "{temp_root / "fundamental_input.json"}"' (no .as_posix yet)
content = content.replace(
    'temp_root / "fundamental_input.json"}"',
    '(temp_root / "fundamental_input.json").as_posix()}"'
)

# Fix f'output_path = "{output_path}"' (plain output_path without .as_posix())
# Only where output_path doesn't already have .as_posix()
import re
# Pattern: {output_path}" without .as_posix
content = re.sub(
    r'\{output_path\}"',
    lambda m: '{output_path.as_posix()}"',
    content,
)

changed = sum(1 for a, b in zip(original.split('\n'), content.split('\n')) if a != b)
print(f"Changed {changed} lines")

if content != original:
    open("tests/test_fundamental_research_input_builder.py", "w", encoding="utf-8").write(content)
    print("File updated.")
else:
    print("No changes.")
