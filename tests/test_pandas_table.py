import json
from pathlib import Path
import logging
import textwrap  # For potentially cleaner printing

# --- Configuration ---
# Assuming this script is saved in FinLens/tests/ and JSON is in FinLens/
_script_dir = Path(__file__).resolve().parent
_project_root = _script_dir.parent
JSON_INPUT_PATH = _project_root / "jpm_nodes_output.json"  #<-- Make sure this path is correct

# --- How many tables to print? ---
MAX_TABLES_TO_PRINT = 5
# --- End Configuration ---

logging.basicConfig(level=logging.INFO, format='%(levelname)s: %(message)s')


def evaluate_table_markdown(json_filepath: Path):
    """Loads the JSON and prints full Markdown content for TABLE nodes."""

    if not json_filepath.exists():
        logging.error(f"Cannot find JSON file at {json_filepath}")
        return

    logging.info(f"Loading nodes from: {json_filepath}")
    try:
        with open(json_filepath, 'r', encoding='utf-8') as f:
            nodes_data = json.load(f)
    except Exception as e:
        logging.error(f"Failed to load or parse JSON: {e}")
        return

    logging.info(f"Loaded {len(nodes_data)} nodes. Searching for tables...")

    table_count = 0
    nodes_by_id = {
        node['node_id']: node
        for node in nodes_data
    }  # For parent lookup

    print("\n--- Sample Table Markdown Content ---")

    for node in nodes_data:
        if node.get('node_type') == "TABLE":
            table_count += 1
            if table_count > MAX_TABLES_TO_PRINT:
                break

            node_id = node.get('node_id', 'N/A')
            parent_id = node.get('parent_node_id', 'N/A')
            level = node.get('level', -1)
            section_id = node.get('section_id', 'N/A')
            order = node.get('order_in_parent', -1)
            markdown_content = node.get('text_content',
                                        '[NO TEXT CONTENT FOUND]')

            parent_info = "N/A"
            if parent_id in nodes_by_id:
                parent_node = nodes_by_id[parent_id]
                parent_info = f"ID={parent_id[:8]}..., Section={parent_node.get('section_id', 'N/A')}, Level={parent_node.get('level', -1)}"

            print(f"\n--- Table {table_count} ---")
            print(f"Node ID : {node_id}")
            print(f"Level   : {level}")
            print(
                f"Section : {section_id}"
            )  # Note: Tables often don't get their own section_id from titles
            print(f"Order   : {order}")
            print(f"Parent  : {parent_info}")
            print(f"Markdown Content:\n")
            # Use textwrap.dedent if markdown has unwanted leading spaces, otherwise just print
            # print(textwrap.dedent(markdown_content))
            print(markdown_content)  # Print the full markdown
            print(f"\n--------------------------")

    if table_count == 0:
        print("No nodes with node_type='TABLE' found.")
    elif table_count > MAX_TABLES_TO_PRINT:
        print(f"\n(Printed first {MAX_TABLES_TO_PRINT} tables found)")

    print("\n--- Table Evaluation Finished ---")


if __name__ == "__main__":
    evaluate_table_markdown(JSON_INPUT_PATH)
