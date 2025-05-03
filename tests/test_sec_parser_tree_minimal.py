# FinLens/tests/test_sec_parser_tree_minimal.py
"""
Minimal test script to run sec-parser (Edgar10KParser)
on specified HTML fixture files, print the resulting Semantic Tree structure,
and save the rendered tree to a text file.

Assumes sec-parser has been installed in editable mode via:
pip install -e ./libs/sec-parser (run from FinLens root after submodule setup)
"""

import sys
import os
import logging
from pathlib import Path
import time
from typing import Optional, List
import json  # Keep json import if needed elsewhere, or remove if not.
import warnings

# --- Configuration ---
SCRIPT_DIR = Path(__file__).parent
FIXTURE_DIR = SCRIPT_DIR / "fixtures"
# Define an output directory for saved trees
OUTPUT_DIR = SCRIPT_DIR / "output"
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)  # Create output dir if needed

# --- Files to Test ---
# Add more filings for broader testing
HTML_FILES_TO_TEST = {
    "jpm 10-K 2024": FIXTURE_DIR / "jpm-20241231.htm",
    "Apple 10-K 2023": FIXTURE_DIR / "aapl-20230930.htm",
    "Google 10-K 2023": FIXTURE_DIR / "goog-20231231.htm",
    "Tesla 10-K 2023": FIXTURE_DIR / "tsla-20231231.htm",
    "ExxonMobil 10-K 2023": FIXTURE_DIR / "xom-20231231.htm",
    "jpm 10-K 2024": FIXTURE_DIR / "jpm-20241231.htm",
    "Apple 10-K 2024": FIXTURE_DIR / "aapl-20240928.htm",
    "Google 10-K 2024": FIXTURE_DIR / "goog-20241231.htm",
    "Tesla 10-K 2024": FIXTURE_DIR / "tsla-20241231.htm",
    "ExxonMobil 10-K 2024": FIXTURE_DIR / "xom-20241231.htm",
}

# --- Setup Logging ---
logging.captureWarnings(True)
log_file_path = Path(__file__).parent / "test_script.log"
# Clear log file at the start of the script run
if log_file_path.exists():
    with log_file_path.open("w"):
        pass

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)-5.5s] %(name)s: %(message)s",
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler(log_file_path)
    ],
)
logger = logging.getLogger("MinimalSecParserTest")  # Simplified name
logging.getLogger("sec_parser").setLevel(logging.WARNING)
# logging.getLogger("sec_parser").setLevel(logging.DEBUG) # Uncomment for fallback details

# --- Import sec-parser Components ---
try:
    import sec_parser as sp
    from sec_parser import (
        Edgar10KParser,
        # Edgar10QParser, # Skipping 10Q for now
        TreeBuilder,
        AbstractSemanticElement,
        SemanticTree)  # Keep original name for type hints

    SEC_PARSER_AVAILABLE = True
    logger.info(
        "Successfully imported sec_parser components (likely via editable install)."
    )
except ImportError as e:
    logger.critical(
        "CRITICAL: Failed to import sec_parser. "
        "Ensure it was added as a submodule in './libs/sec-parser' "
        "and installed editable ('pip install -e ./libs/sec-parser') "
        f"in your active virtual environment. Error: {e}",
        exc_info=True,
    )
    SEC_PARSER_AVAILABLE = False


# --- Helper Function to Save Output ---
# Updated to only handle string content since JSON is removed
def save_text_output(content: str, output_path: Path):
    """Saves text content to the specified path."""
    try:
        with output_path.open("w", encoding="utf-8") as f:
            f.write(content)
        return True
    except Exception as e:
        logger.error(f"Failed to save output to {output_path}: {e}",
                     exc_info=True)
        return False


# --- Main Test Logic ---
def run_parser_test():  # Renamed function
    """
    Runs the Edgar10KParser on configured fixture files,
    and saves the rendered tree to a text file.
    """
    if not SEC_PARSER_AVAILABLE:
        logger.error("Cannot run test, sec-parser components unavailable.")
        sys.exit(1)

    try:
        parser_10k = Edgar10KParser()
        tree_builder = TreeBuilder()
        logger.info("Instantiated Edgar10KParser and TreeBuilder.")
    except Exception as e:
        logger.error(f"Failed to instantiate sec_parser classes: {e}",
                     exc_info=True)
        sys.exit(1)

    total_files = len(HTML_FILES_TO_TEST)
    files_processed = 0
    all_tests_start_time = time.time()

    for test_name, html_path in HTML_FILES_TO_TEST.items():
        files_processed += 1
        test_case_start_time = time.time()
        output_base_name = html_path.stem

        print("\n" + "#" * 80)
        logger.info(
            f"## Processing Test Case ({files_processed}/{total_files}): '{test_name}' ##"
        )
        logger.info(f"## HTML Source: {html_path.name} ##")
        print("#" * 80)

        if not html_path.is_file():
            logger.error(f"SKIPPING: HTML fixture file not found: {html_path}")
            print("#" * 80)
            continue

        # Read HTML Content
        html_content: str
        try:
            try:
                html_content = html_path.read_text(encoding="utf-8")
            except UnicodeDecodeError:
                logger.warning(
                    f"UTF-8 decode failed for {html_path.name}, trying latin-1..."
                )
                html_content = html_path.read_text(encoding="latin-1")
            logger.info(
                f"Successfully read {len(html_content):,} characters from {html_path.name}"
            )
        except Exception as e:
            logger.error(
                f"SKIPPING: Failed to read HTML file {html_path}: {e}",
                exc_info=True)
            print("#" * 80)
            continue

        # --- Process, Render, and Save for Edgar10KParser ---
        print("\n" + "=" * 30 + " 10-K Parser Output " + "=" * 30)
        tree_10k: Optional[SemanticTree] = None  # Type hint uses original name
        elements_10k: Optional[List[AbstractSemanticElement]] = None
        parsing_start_time = time.time()
        try:
            logger.info("Running 10-K Parser -> parse()...")
            elements_10k = parser_10k.parse(html_content)
            parsing_duration = time.time() - parsing_start_time
            logger.info(
                f"10-K Parser found {len(elements_10k) if elements_10k else 0} elements in {parsing_duration:.2f}s."
            )

            if elements_10k:
                building_start_time = time.time()
                logger.info("Running 10-K Parser -> build_tree()...")
                tree_10k = tree_builder.build(elements_10k)
                building_duration = time.time() - building_start_time
                logger.info(
                    f"10-K Tree building complete in {building_duration:.2f}s."
                )
            else:
                logger.warning("10-K Parser produced no elements.")
        except Exception as e:
            logger.error(f"Error during 10-K parsing/building: {e}",
                         exc_info=True)

        if tree_10k and sp:
            # Attempt to save rendered text output
            logger.info("Attempting to save 10-K rendered output...")
            try:
                render_save_start = time.time()
                rendered_tree_10k_str = sp.render(tree_10k,
                                                  pretty=True,
                                                  verbose=True)
                output_path = OUTPUT_DIR / f"{output_base_name}_10k_render.txt"
                if save_text_output(rendered_tree_10k_str, output_path):
                    render_save_duration = time.time() - render_save_start
                    logger.info(
                        f"Saved {output_path.name} in {render_save_duration:.2f}s"
                    )
                # else: Error logged by save_text_output

            except Exception as e:
                logger.error(f"Failed to render or save 10-K tree text: {e}",
                             exc_info=True)

            # --- JSON Saving Block Removed ---

        else:
            logger.warning(
                "No tree generated for 10-K Parser.")  # Adjusted message
        print("=" * 78)  # End 10-K section

        test_case_duration = time.time() - test_case_start_time
        logger.info(
            f"## Finished Test Case '{test_name}' in {test_case_duration:.2f} seconds ##"
        )
        print("#" * 80)  # End Test Case section

    all_tests_duration = time.time() - all_tests_start_time
    logger.info(f"--- sec-parser Test Finished ({total_files} files) ---"
                )  # Simplified name
    logger.info(f"Total processing time: {all_tests_duration:.2f} seconds.")
    logger.info(f"Rendered output files saved in: {OUTPUT_DIR.resolve()}")
    logger.info(f"Log file saved in: {log_file_path.resolve()}")


# --- Execution Guard ---
if __name__ == "__main__":
    run_parser_test()  # Renamed function call
