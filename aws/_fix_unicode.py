"""Fix non-ASCII in print/log runtime strings in aws/ Python files."""
import os, re

# Characters to replace in runtime output strings (print/log lines)
REPLACEMENTS = [
    ("\u2014", "-"),   # em dash
    ("\u2013", "-"),   # en dash
    ("\u2192", "->"),  # right arrow
    ("\u2190", "<-"),  # left arrow
    ("\u2026", "..."), # ellipsis
    ("\u2713", "[OK]"),# check mark
    ("\u2717", "[X]"), # cross
    ("\u2705", "[OK]"),# green tick
    ("\u2714", "[OK]"),# heavy check
    ("\u00b7", "."),   # middle dot
    # box-drawing in print strings (replace whole box with ASCII)
    ("\u2554", "+"),   # double top-left corner
    ("\u2557", "+"),   # double top-right corner
    ("\u255a", "+"),   # double bottom-left
    ("\u255d", "+"),   # double bottom-right
    ("\u2550", "="),   # double horizontal
    ("\u2551", "|"),   # double vertical
    ("\u2500", "-"),   # single horizontal
    ("\u2502", "|"),   # single vertical
    ("\u250c", "+"),   # single top-left
    ("\u2510", "+"),   # single top-right
    ("\u2514", "+"),   # single bottom-left
    ("\u2518", "+"),   # single bottom-right
    ("\u251c", "+"),   # single left tee
    ("\u2524", "+"),   # single right tee
    ("\u252c", "+"),   # single top tee
    ("\u2534", "+"),   # single bottom tee
    ("\u253c", "+"),   # single cross
    ("\u2569", "+"),   # double bottom tee
    ("\u2566", "+"),   # double top tee
    ("\u256c", "+"),   # double cross
    ("\u25bc", "v"),   # down-pointing triangle
    ("\u2193", "v"),   # down arrow
    ("\u2191", "^"),   # up arrow
]

# Files where print() strings need fixing (runtime output on Windows terminal)
TARGET_FILES = [
    "aws/aws_main.py",
    "aws/grant_permissions.py",
    "aws/setup_credentials.py",
]

for path in TARGET_FILES:
    if not os.path.exists(path):
        continue
    original = open(path, encoding="utf-8").read()
    lines = original.splitlines()
    new_lines = []
    for line in lines:
        stripped = line.lstrip()
        # Only clean lines that generate terminal output
        if any(stripped.startswith(p) for p in
               ("print(", 'f"', "f'", "log.", "raise ", "return f")):
            for old, new in REPLACEMENTS:
                line = line.replace(old, new)
        new_lines.append(line)
    new_text = "\n".join(new_lines)
    if new_text != original:
        open(path, "w", encoding="utf-8").write(new_text)
        print("Fixed:", path)
    else:
        print("Clean:", path)

print("Done.")
