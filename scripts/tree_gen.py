import os
import sys

IGNORE_DIRS = {"__pycache__"}
IGNORE_PREFIXES = {'.'}

def get_py_description(filepath: str) -> str:
    """Return the 2nd line of a Python file's top-level docstring if present."""
    try:
        with open(filepath, 'r', encoding='utf-8') as f:
            lines = f.readlines()
        if len(lines) >= 2:
            # Strip whitespace and quotes from likely docstring content
            desc = lines[1].strip().strip('\'"')
            return f" — {desc}" if desc else ""
    except Exception:
        pass
    return ""

def print_tree(start_path: str, prefix: str = ""):
    """Recursively print ASCII directory tree."""
    try:
        entries = sorted(os.listdir(start_path))
    except PermissionError:
        print(prefix + "[Permission Denied]")
        return

    # Filter out ignored entries
    entries = [
        e for e in entries
        if not any(e.startswith(p) for p in IGNORE_PREFIXES)
        and e not in IGNORE_DIRS
    ]

    entries_count = len(entries)
    for i, entry in enumerate(entries):
        path = os.path.join(start_path, entry)
        connector = "└── " if i == entries_count - 1 else "├── "
        display_name = entry

        # If it's a Python file, try to append the 2nd line description
        if os.path.isfile(path) and entry.endswith(".py"):
            desc = get_py_description(path)
            display_name += desc

        print(prefix + connector + display_name)

        if os.path.isdir(path):
            extension = "    " if i == entries_count - 1 else "│   "
            print_tree(path, prefix + extension)

if __name__ == "__main__":
    root = sys.argv[1] if len(sys.argv) > 1 else "."
    print(root)
    print_tree(root)
