import os
from pathlib import Path
from typing import List, Optional

# --- Imports for sec-parser ---
SEC_PARSER_AVAILABLE = False
try:
    from sec_parser import Edgar10KParser, TreeBuilder, SemanticTree, render
    from sec_parser.semantic_elements.abstract_semantic_element import AbstractSemanticElement
    from sec_parser.semantic_tree.tree_node import TreeNode  # For type hinting
    SEC_PARSER_AVAILABLE = True
    print("Successfully imported sec-parser components.")
except ImportError as e:
    print(f"ERROR: sec-parser library not found or core classes missing: {e}. "
          "Please ensure it's installed and accessible in your PYTHONPATH.")


def generate_sec_parser_tree_representation(html_fixture_path: str,
                                            output_text_filepath: str) -> bool:
    """
    Parses an HTML fixture using Edgar10KParser, builds a SemanticTree,
    renders it to a string, and writes it to a file.

    Args:
        html_fixture_path: Path to the 10-K HTML fixture file.
        output_text_filepath: Path to save the rendered semantic tree.

    Returns:
        True if successful, False otherwise.
    """
    if not SEC_PARSER_AVAILABLE:
        print(
            "Cannot generate semantic tree: sec-parser library is not available."
        )
        return False

    print(f"Processing HTML file: {html_fixture_path}")

    try:
        with open(html_fixture_path, 'r', encoding='utf-8') as f:
            html_content = f.read()
        print(
            f"Successfully read HTML file (length: {len(html_content)} chars)."
        )
    except FileNotFoundError:
        print(f"ERROR: Fixture file not found at {html_fixture_path}")
        return False
    except Exception as e:
        print(f"ERROR: Could not read HTML file {html_fixture_path}: {e}")
        return False

    try:
        # 1. Initialize the 10-K parser
        parser = Edgar10KParser()
        print("Initialized Edgar10KParser.")

        # 2. Parse the HTML to get a flat list of semantic elements
        print("Parsing HTML to semantic elements...")
        elements: List[AbstractSemanticElement] = parser.parse(html_content)
        print(f"Parsed {len(elements)} semantic elements.")

        # 3. Build the semantic tree from the elements
        print("Building semantic tree...")
        tree_builder = TreeBuilder()
        semantic_tree: SemanticTree = tree_builder.build(elements)

        # --- CORRECTED LINE ---
        # Convert generator to list to get its length
        root_nodes_list: List[TreeNode] = list(semantic_tree.nodes)
        print(f"Semantic tree built. Root nodes: {len(root_nodes_list)}")
        # --- END CORRECTION ---

        # 4. Render the tree to a string representation
        #    The render function expects the SemanticTree object directly.
        print("Rendering semantic tree to string...")
        tree_representation: str = render(
            semantic_tree)  # Pass the SemanticTree object
        print(
            f"Tree rendered to string (length: {len(tree_representation)} chars)."
        )

        # 5. Write the string representation to the output file
        output_dir = Path(output_text_filepath).parent
        output_dir.mkdir(parents=True,
                         exist_ok=True)  # Ensure output directory exists

        with open(output_text_filepath, 'w', encoding='utf-8') as f:
            f.write(tree_representation)
        print(
            f"Successfully wrote semantic tree representation to: {output_text_filepath}"
        )
        return True

    except Exception as e:
        print(
            f"ERROR: An error occurred during sec-parser processing or rendering: {e}"
        )
        import traceback
        traceback.print_exc()  # Print full traceback for debugging
        return False


if __name__ == "__main__":
    try:
        script_dir = Path(__file__).resolve().parent
        project_root_candidate = script_dir
        max_levels_up = 3
        for _ in range(max_levels_up):
            if project_root_candidate.name == "FinLens":
                break
            if project_root_candidate.parent == project_root_candidate:
                project_root_candidate = Path.cwd()
                break
            project_root_candidate = project_root_candidate.parent

        if project_root_candidate.name != "FinLens":
            project_root_candidate = Path.cwd()
            if project_root_candidate.name != "FinLens" and (
                    project_root_candidate / "FinLens").is_dir():
                project_root_candidate = project_root_candidate / "FinLens"

        base_fixtures_path = project_root_candidate / "tests" / "fixtures"
        output_directory = project_root_candidate / "tests" / "output"
    except NameError:
        print(
            "Warning: __file__ not defined. Using paths relative to current working directory (FinLens/tests/fixtures and FinLens/tests/output)."
        )
        base_fixtures_path = Path("FinLens") / "tests" / "fixtures"
        output_directory = Path("FinLens") / "tests" / "output"

    if not SEC_PARSER_AVAILABLE:
        print(
            "CRITICAL ERROR: sec-parser library is not available. Cannot generate tree."
        )
    elif not base_fixtures_path.is_dir():
        print(
            f"ERROR: Fixtures directory not found at '{base_fixtures_path.resolve()}'"
        )
    else:
        print(f"Using fixtures directory: {base_fixtures_path.resolve()}")

        jpm_fixture_filename = "jpm-20241231.htm"
        jpm_fixture_path = str(base_fixtures_path / jpm_fixture_filename)

        output_filename = "jpm_sec_parser_semantic_tree.txt"
        output_filepath = str(output_directory / output_filename)

        if Path(jpm_fixture_path).is_file():
            print(
                f"\n--- Generating sec-parser Semantic Tree for: {jpm_fixture_filename} ---"
            )
            success = generate_sec_parser_tree_representation(
                jpm_fixture_path, output_filepath)
            if success:
                print(f"\nSemantic tree saved to {output_filepath}")
            else:
                print(
                    f"\nFailed to generate semantic tree for {jpm_fixture_filename}"
                )
        else:
            print(
                f"ERROR: JPM fixture file '{jpm_fixture_filename}' not found in '{base_fixtures_path}'."
            )
