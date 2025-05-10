import os
from pathlib import Path
from typing import List
# Assuming AbstractSemanticElement is a base class or type hint from sec_parser
from sec_parser.semantic_elements.abstract_semantic_element import AbstractSemanticElement

# --- Imports for sec-parser ---
SEC_PARSER_AVAILABLE = False
try:
    from sec_parser import Edgar10KParser
    # PageNumberElement is the specific type we're looking for
    from sec_parser.semantic_elements.semantic_elements import PageNumberElement
    SEC_PARSER_AVAILABLE = True
except ImportError:
    print(
        "ERROR: sec-parser library not found or core classes (Edgar10KParser, PageNumberElement) missing. "
        "Please ensure it's installed and accessible in your PYTHONPATH.")


def get_semantic_elements_from_10k_fixture(
        fixture_path: str) -> List[AbstractSemanticElement]:
    """
    Reads a 10-K HTML fixture file and processes it with Edgar10KParser.
    Returns a list of AbstractSemanticElement.
    """
    if not SEC_PARSER_AVAILABLE:
        print("Skipping fixture processing: sec-parser is not available.")
        return []
    try:
        with open(fixture_path, 'r', encoding='utf-8') as f:
            html_content = f.read()

        # Initialize the 10-K parser
        parser = Edgar10KParser()
        elements: List[AbstractSemanticElement] = parser.parse(html_content)
        return elements
    except FileNotFoundError:
        print(f"ERROR: Fixture file not found at {fixture_path}")
        return []
    except Exception as e:
        print(
            f"ERROR: Failed to parse {fixture_path} with Edgar10KParser: {e}")
        return []


def test_visual_page_number_extraction(fixture_path: str):
    """
    Processes a 10-K fixture file using Edgar10KParser and
    extracts and prints any PageNumberElement instances found.
    """
    print(
        f"\n=== Processing 10-K Fixture for Visual Page Numbers: {fixture_path} ==="
    )
    semantic_elements = get_semantic_elements_from_10k_fixture(fixture_path)

    if not semantic_elements:
        print("  No semantic elements found or error in parsing this fixture.")
        return

    page_number_elements_found = 0
    for i, element in enumerate(semantic_elements):
        # Check if the element is an instance of PageNumberElement
        if isinstance(element, PageNumberElement):
            page_number_elements_found += 1
            page_text = element.text.strip() if hasattr(
                element, 'text') else "[NO TEXT]"
            print(f"  Found PageNumberElement (Index {i}): '{page_text}'")
            # Optional: Inspect the HTML tag for context
            if hasattr(element, 'html_tag') and hasattr(
                    element.html_tag, '_bs4'):
                print(
                    f"    HTML tag snippet: {str(element.html_tag._bs4)[:100]}..."
                )

    print(f"\n=== Finished {fixture_path} ===")
    print(
        f"Total PageNumberElements identified by sec-parser: {page_number_elements_found}"
    )


# --- Main execution for testing ---
if __name__ == "__main__":
    # Directly use "FinLens/" as the base for the fixtures path.
    # This assumes the script is run from a directory that is a parent of "FinLens",
    # or "FinLens" is in the current working directory.
    base_fixtures_path = Path("tests") / "fixtures"

    if not SEC_PARSER_AVAILABLE:
        print(
            "CRITICAL ERROR: sec-parser library is not available. Testing cannot continue."
        )
    elif not base_fixtures_path.is_dir():
        print(
            f"ERROR: Fixtures directory not found at '{base_fixtures_path.resolve()}'"
        )
        print(
            "       Please ensure the path is correct. This script expects to find 'FinLens/tests/fixtures/' "
            "relative to its execution context or as an absolute path if modified."
        )
    else:
        print(
            f"Fixtures directory determined to be: {base_fixtures_path.resolve()}"
        )

        # --- Test the JPM 10-K fixture ---
        jpm_fixture_filename = "goog-20241231.htm"
        jpm_fixture_path = base_fixtures_path / jpm_fixture_filename

        if jpm_fixture_path.is_file():
            test_visual_page_number_extraction(str(jpm_fixture_path))
        else:
            print(
                f"ERROR: The JPM fixture to test '{jpm_fixture_filename}' was not found in '{base_fixtures_path}'."
            )
            print(
                "       Please ensure the file exists or check the 'base_fixtures_path' logic if it's incorrect."
            )
