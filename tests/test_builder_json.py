import json
from pathlib import Path
from collections import defaultdict

# --- Configuration ---
# Adjust path relative to where you save/run this script,
# or provide the full absolute path.
# Assuming this script is saved in FinLens/tests/ and JSON is in FinLens/
_script_dir = Path(__file__).resolve().parent
_project_root = _script_dir.parent
JSON_INPUT_PATH = _project_root / "jpm_nodes_output.json"
# --- End Configuration ---


def evaluate_structure(json_filepath: Path):
    """Loads the JSON and prints Part/Item hierarchy."""

    if not json_filepath.exists():
        print(f"ERROR: Cannot find JSON file at {json_filepath}")
        return

    print(f"Loading nodes from: {json_filepath}")
    try:
        with open(json_filepath, 'r', encoding='utf-8') as f:
            nodes_data = json.load(f)
    except Exception as e:
        print(f"ERROR: Failed to load or parse JSON: {e}")
        return

    print(f"Loaded {len(nodes_data)} nodes.")

    # Create lookup dictionaries
    nodes_by_id = {node['node_id']: node for node in nodes_data}
    nodes_by_parent_id = defaultdict(list)
    for node in nodes_data:
        parent_id = node.get('parent_node_id')
        if parent_id:
            nodes_by_parent_id[parent_id].append(node)

    # Find the root node
    root_node = next(
        (node
         for node in nodes_data if node.get('node_type') == 'DOCUMENT_ROOT'),
        None)
    if not root_node:
        print("ERROR: Cannot find DOCUMENT_ROOT node.")
        return

    print("\n--- Part and Item Hierarchy Evaluation ---")

    # Function to recursively print structure
    def print_node_and_children(node_id, indent_level=0):
        node = nodes_by_id.get(node_id)
        if not node:
            print(f"{'  ' * indent_level}ERROR: Node ID {node_id} not found.")
            return

        node_type = node.get('node_type')
        section_id = node.get('section_id', 'N/A')
        title = node.get('title', '')
        level = node.get('level', -1)
        parent_id = node.get('parent_node_id', 'None')

        # Check if it's a Part or Item title we want to display
        is_part_or_item = node_type == "SECTION_TITLE" and \
                          section_id != 'root' and \
                          (section_id.startswith("part") or section_id.startswith("item"))

        if is_part_or_item:
            parent_info = ""
            if parent_id in nodes_by_id:
                parent_node = nodes_by_id[parent_id]
                parent_info = f"(Parent: ID={parent_id[:8]}..., Section={parent_node.get('section_id', 'N/A')}, Level={parent_node.get('level', -1)})"
            else:
                parent_info = f"(Parent ID: {parent_id})"  # Should ideally not happen for non-root

            # Determine expected level (approximate)
            expected_level = 1 if section_id.startswith(
                "part") and "item" not in section_id else 2
            level_marker = "OK" if level == expected_level else f"WARN (Expected ~{expected_level})"

            print(
                f"{'  ' * indent_level}Node ID: {node_id[:8]}... | Level: {level} [{level_marker}] | Section: {section_id:<12} | Title: '{title[:50]}...' {parent_info}"
            )

            # Get children and sort them by order_in_parent if available
            children = sorted(nodes_by_parent_id.get(node_id, []),
                              key=lambda x: x.get('order_in_parent', 0))
            for child in children:
                # Only recurse if the child is also a Part or Item (to keep output focused)
                child_type = child.get('node_type')
                child_section_id = child.get('section_id', '')
                if child_type == "SECTION_TITLE" and (
                        child_section_id.startswith("part")
                        or child_section_id.startswith("item")):
                    print_node_and_children(child['node_id'], indent_level + 1)

    # Start printing from the children of the root
    root_children = sorted(nodes_by_parent_id.get(root_node['node_id'], []),
                           key=lambda x: x.get('order_in_parent', 0))
    for child in root_children:
        # Only start traversal from SECTION_TITLE nodes directly under root
        if child.get('node_type') == 'SECTION_TITLE':
            print_node_and_children(child['node_id'], 0)

    print("\n--- Evaluation Finished ---")


if __name__ == "__main__":
    evaluate_structure(JSON_INPUT_PATH)
