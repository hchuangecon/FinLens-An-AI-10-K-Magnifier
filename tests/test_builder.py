import logging
import os
import sys
import json
import time
from pathlib import Path  # Make sure Path is imported
from typing import Type, List, Optional, Dict, Any  # Make sure these are imported

# --- Path Setup (Should be correct from previous version) ---
try:
    _script_path = Path(__file__).resolve()
    _tests_dir = _script_path.parent
    _project_root = _tests_dir.parent  # FinLens/
    _src_path = _project_root / 'src'
    _libs_path = _project_root / 'libs'
    if str(_src_path) not in sys.path: sys.path.insert(0, str(_src_path))
    if str(_libs_path) not in sys.path: sys.path.insert(0, str(_libs_path))
    print(
        f"--- Project Root ---\nROOT: {_project_root}\n--- Prepended paths ---\nSRC: {_src_path}\nLIB: {_libs_path}\n{'-'*20}"
    )
except NameError:
    _project_root = Path(os.getcwd())
    _src_path = _project_root / 'src'
    _libs_path = _project_root / 'libs'
    if str(_src_path) not in sys.path: sys.path.insert(0, str(_src_path))
    if str(_libs_path) not in sys.path: sys.path.insert(0, str(_libs_path))
    print(
        f"--- Project Root (guessed) ---\nROOT: {_project_root}\n--- Prepended paths ---\nSRC: {_src_path}\nLIB: {_libs_path}\n{'-'*20}"
    )

# --- Logging Setup ---
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# --- Imports ---
try:
    from src.phase2_parsing.node_builders.sec_parser_node_builder import SecParserNodeBuilder
    from src.phase2_parsing.types.models import FinLensNode
    from sec_parser import Edgar10KParser, Edgar10QParser, SemanticTree, TreeBuilder
    from sec_parser.processing_engine.core import AbstractSemanticElementParser
    from sec_parser.semantic_elements import AbstractSemanticElement

    BUILDER_IMPORTED = True
except ImportError as e:
    logger.critical(f"Failed to import necessary modules: {e}.", exc_info=True)
    BUILDER_IMPORTED = False
    sys.exit(1)

# --- Configuration ---
SCRIPT_DIR = Path(__file__).resolve().parent
JPM_HTML_PATH = SCRIPT_DIR / "./fixtures/jpm-20241231.htm"
OUTPUT_JSON_PATH = _project_root / "jpm_nodes_output.json"


# --- CORRECTED run_test function ---
def run_test(html_filepath: Path,
             output_json_filepath: Path):  # Added Path type hints
    """
    Runs the parsing, building, verification, and output process.
    """
    # Check if imports succeeded earlier
    if not BUILDER_IMPORTED:
        logger.error(
            "Required libraries (sec-parser or FinLens components) not imported correctly. Exiting."
        )
        sys.exit(1)

    logger.info(f"Starting test with {html_filepath.name}")
    start_time = time.time()

    # --- 1. Read HTML ---
    html_content: Optional[str] = None
    encodings_to_try = ['utf-8', 'latin-1', 'iso-8859-1']
    for enc in encodings_to_try:
        try:
            with open(html_filepath, "r", encoding=enc) as f:
                html_content = f.read()
            logger.info(f"Successfully read HTML file with encoding '{enc}'.")
            break
        except UnicodeDecodeError:
            logger.debug(f"Encoding {enc} failed, trying next...")
        except Exception as e:
            logger.error(f"Error reading HTML file {html_filepath}: {e}",
                         exc_info=True)
            sys.exit(1)
    if not html_content:
        logger.error(
            f"Could not decode HTML file {html_filepath} with any tried encoding."
        )
        sys.exit(1)

    semantic_tree: Optional[SemanticTree] = None

    # --- 2. Determine Parser and Parse HTML ---
    semantic_tree: Optional[SemanticTree] = None  # Initialize
    elements: List[AbstractSemanticElement] = []  # Initialize elements list
    parse_start_time = time.time()
    try:
        # --- (Metadata and Parser Class selection stays the same) ---
        doc_meta: Dict[str, Any] = {
            "accession_number": html_filepath.stem,
            "filename_base": html_filepath.stem,
            "cik": "19617",  # Example
            "form_type": "10-K",  # Example
            "filing_date": "2025-02-14",  # Example
            "fiscal_year_end_date": "2024-12-31"  # Example
        }
        form_type = doc_meta.get("form_type", "").upper()
        parser_class: Type[AbstractSemanticElementParser]
        if form_type.startswith("10-K"): parser_class = Edgar10KParser
        elif form_type.startswith("10-Q"): parser_class = Edgar10QParser
        else:
            logger.warning(
                f"Unsupported form_type '{form_type}', defaulting to Edgar10KParser."
            )
            parser_class = Edgar10KParser

        logger.info(f"Initializing {parser_class.__name__}...")
        sec_parser_instance = parser_class()

        # --- Step 2a: Parse Elements ---
        logger.info("Parsing elements...")
        elements = sec_parser_instance.parse(
            html_content)  # Returns a list of elements
        logger.info(f"Parsed {len(elements)} semantic elements."
                    )  # len() works on the list

        # --- Step 2b: Build Tree ---
        logger.info("Building semantic tree...")
        tree_builder = TreeBuilder()
        semantic_tree = tree_builder.build(
            elements)  # Build tree from elements list

        nodes_type = type(semantic_tree.nodes)
        logger.info(f"Type of semantic_tree.nodes: {nodes_type}"
                    )  # Should now be <class 'generator'>

        parse_time = time.time() - parse_start_time
        logger.info(
            f"sec-parser (elements + tree) processing finished in {parse_time:.2f} seconds."
            # Cannot reliably report root node count without consuming generator
        )

    except Exception as e:
        logger.error(f"sec-parser processing failed: {e}", exc_info=True)
        sys.exit(1)  # Exit if parsing/tree building fails

    # Check if semantic_tree was successfully created before proceeding
    if semantic_tree is None:
        logger.error("Semantic tree was not created due to previous errors.")
        sys.exit(1)

    # --- 3. Build FinLensNode Tree ---
    finlens_nodes: List[FinLensNode] = []
    root_finlens_node: Optional[FinLensNode] = None
    logger.info("Initializing SecParserNodeBuilder...")
    build_start_time = time.time()
    try:
        node_builder = SecParserNodeBuilder()
        finlens_nodes, root_finlens_node = node_builder.build_tree(
            semantic_tree, doc_meta)
        build_time = time.time() - build_start_time
        logger.info(
            f"Node builder finished in {build_time:.2f} seconds. Created {len(finlens_nodes)} FinLens nodes."
        )
    except Exception as e:
        logger.error(f"Node builder failed: {e}", exc_info=True)
        sys.exit(1)

    # --- 4. Verification Checks (REVISED) ---
    if not finlens_nodes or not root_finlens_node:
        logger.error("Node building did not produce nodes or a root node.")
        sys.exit(1)

    print("\n--- Verification ---")
    item5_node: Optional[FinLensNode] = None
    part2_node: Optional[FinLensNode] = None
    nodes_by_id = {
        node.node_id: node
        for node in finlens_nodes
    }  # Build lookup map

    # Find the relevant nodes
    for node in finlens_nodes:
        if item5_node is None and \
           node.node_type == "SECTION_TITLE" and \
           node.title and \
           node.title.strip().startswith("Item 5."):
            item5_node = node
            logger.info(
                f"Found potential Item 5 node: ID={node.node_id}, Level={node.level}, ParentID={node.parent_node_id}"
            )
        if part2_node is None and \
           node.node_type == "SECTION_TITLE" and \
           node.section_id == "part2":
            part2_node = node
            logger.info(
                f"Found potential Part II node: ID={node.node_id}, Level={node.level}, Title='{node.title}'"
            )
        if item5_node and part2_node: break  # Optimization

    # Perform Checks
    passed = True
    if not item5_node:
        print("\033[91mFAILURE: Could not find 'Item 5.' node.\033[0m")
        passed = False
    if not part2_node:
        logger.warning(
            "Could not find 'Part II' node (section_id='part2'). Parent check will rely on found parent's section_id."
        )
        # Don't fail overall just because Part II node wasn't found separately,
        # focus on Item 5's parent properties.
        pass

    if item5_node:
        print(
            f"\nFound Item 5 node: ID={item5_node.node_id}, Level={item5_node.level}, ParentID={item5_node.parent_node_id}"
        )
        if part2_node:  # Log if Part II node was found for reference
            print(
                f"Found Part II node: ID={part2_node.node_id}, Level={part2_node.level}, SectionID={part2_node.section_id}"
            )

        # Check 1: Item 5 Level
        expected_level = 2
        if item5_node.level == expected_level:
            print(
                f"\033[92m  Level Check PASSED:\033[0m Item 5 node is at Level {item5_node.level}."
            )
        else:
            print(
                f"\033[91m  Level Check FAILED:\033[0m Item 5 node is at Level {item5_node.level} (Expected {expected_level})."
            )
            passed = False

        # Check 2: Parentage (Check parent's section_id and level)
        actual_parent = nodes_by_id.get(item5_node.parent_node_id)
        if actual_parent:
            print(
                f"  Actual Parent Details: ID={actual_parent.node_id}, Title='{actual_parent.title}', Type={actual_parent.node_type}, SectionID={actual_parent.section_id}, Level={actual_parent.level}"
            )
            # Check 2a: Parent Section ID
            if actual_parent.section_id == "part2":
                print(
                    f"\033[92m  Parent Section Check PASSED:\033[0m Item 5 parent has correct section_id ('part2')."
                )
            else:
                print(
                    f"\033[91m  Parent Section Check FAILED:\033[0m Item 5 parent has section_id '{actual_parent.section_id}' (Expected 'part2')."
                )
                passed = False
            # Check 2b: Parent Level
            if actual_parent.level == 1:
                print(
                    f"\033[92m  Parent Level Check PASSED:\033[0m Item 5 parent is at Level {actual_parent.level}."
                )
            else:
                print(
                    f"\033[91m  Parent Level Check FAILED:\033[0m Item 5 parent is at Level {actual_parent.level} (Expected 1)."
                )
                passed = False
        else:
            print(
                f"\033[91m  Parent Check FAILED:\033[0m Could not find parent node with ID {item5_node.parent_node_id} in the list."
            )
            passed = False

    print("\n--- Verification Summary ---")
    if passed:
        print("\033[92mOverall Verification PASSED\033[0m")
    else:
        print("\033[91mOverall Verification FAILED\033[0m")
    # --- End Revised Verification Logic ---

    # --- 5. Write Output JSON ---
    # Ensure the output directory exists
    output_json_filepath.parent.mkdir(parents=True,
                                      exist_ok=True)  # Use the argument name
    logger.info(f"Writing output node list to {output_json_filepath}..."
                )  # Use the argument name
    try:
        nodes_as_dicts = [
            node.model_dump(mode='json') for node in finlens_nodes
        ]
        with open(output_json_filepath, 'w',
                  encoding='utf-8') as f:  # Use the argument name
            json.dump(nodes_as_dicts, f, indent=2, ensure_ascii=False)
        logger.info("Output JSON written successfully.")
    except AttributeError:  # Fallback for Pydantic v1
        try:
            logger.warning("Using Pydantic v1 .dict() for serialization.")
            nodes_as_dicts = [node.dict() for node in finlens_nodes]
            with open(output_json_filepath, 'w',
                      encoding='utf-8') as f:  # Use the argument name
                json.dump(nodes_as_dicts, f, indent=2, ensure_ascii=False)
            logger.info("Output JSON written successfully (using .dict()).")
        except Exception as e_dict:
            logger.error(
                f"Error serializing nodes to JSON (tried model_dump and dict): {e_dict}",
                exc_info=True)
    except Exception as e:
        logger.error(f"Failed to write output JSON: {e}", exc_info=True)

    logger.info("\n--- Test Finished ---")


# --- Main Execution ---
if __name__ == "__main__":
    # Check if HTML file exists before running
    if not JPM_HTML_PATH.exists():
        logger.error(
            f"Input HTML file not found at calculated path: {JPM_HTML_PATH}")
        logger.error(
            "Please ensure the file exists or adjust the path in the script.")
        sys.exit(1)

    # Call run_test with the Path objects
    run_test(JPM_HTML_PATH, OUTPUT_JSON_PATH)
