# src/phase2_parsing/services/processor_service.py
import logging
import json
import time
import sys
import os
import re
# Removed tempfile and BytesIO imports as they were for docling/pdf path
from pathlib import Path
from typing import Dict, List, Optional, Tuple, Any, Union
import traceback

# --- Parser Choice Configuration ---
# Set to use sec-parser exclusively for now
PARSER_CHOICE = "sec_parser"
# ---------------------------------

# --- HTML to PDF Conversion ---
# Removed xhtml2pdf import and logic as it's no longer needed
# -----------------------------

# --- sec-parser Imports ---
SEC_PARSER_AVAILABLE = False

# Always try to import sec-parser if it's the chosen method
if PARSER_CHOICE == "sec_parser":
    try:
        from sec_parser import SecParser
        from sec_parser.semantic_elements.abstract_semantic_element import AbstractSemanticElement
        SEC_PARSER_AVAILABLE = True
        logging.info("sec-parser library found.")
    except ImportError:
        SEC_PARSER_AVAILABLE = False
        # Make this a critical error if sec-parser is the only choice
        logging.critical(
            "sec-parser library not found, but it is the selected parser. Install with: pip install sec-parser"
        )

        sys.exit(1)  # Exit if the required parser isn't available
# --------------------------

# --- Docling Imports (Conditional - Not Used if PARSER_CHOICE="sec_parser") ---
DOCLING_AVAILABLE = False
DoclingWrapper = None
# Keep these imports conditional in case you switch back later
if PARSER_CHOICE == "docling":  # Only import if explicitly chosen
    try:
        from src.phase2_parsing.parsers.docling_wrapper import DoclingWrapper  # Your wrapper
        # Import specific backends if needed by the docling path
        from docling.backend.html_backend import HTMLDocumentBackend
        DOCLING_AVAILABLE = True
        logging.info("Docling components found (for docling path).")
    except ImportError as e:
        logging.warning(
            f"Could not import Docling components (needed for 'docling' mode): {e}"
        )
        DOCLING_AVAILABLE = False
        DoclingWrapper = None
# ----------------------------------------------------

# --- Add src directory to sys.path if needed ---
try:
    _project_root = Path(__file__).resolve().parent.parent.parent
    if str(_project_root) not in sys.path:
        sys.path.insert(0, str(_project_root))
        print(f"--- Prepended project root path: {_project_root} ---")
except NameError:
    _project_root = Path('.').resolve()
    if str(_project_root) not in sys.path:
        sys.path.insert(0, str(_project_root))
        print(f"--- Prepended assumed project root path: {_project_root} ---")
# ------------------------------------------------

# --- Setup Logging ---
try:
    from src.config.logging_config import setup_logging
    setup_logging()
except ImportError:
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        handlers=[logging.StreamHandler(sys.stdout)])
    logging.warning("Could not import setup_logging, using basicConfig.")
# ---------------------

logger = logging.getLogger(__name__)

# --- Import FinLens Components (Common) ---
try:
    from src.config.settings import get_settings, AppSettings
    from src.phase2_parsing.extractors.metadata_extractor import MetadataExtractor
    from src.phase2_parsing.extractors.toc_extractor import ToCExtractor
    from src.phase2_parsing.node_builders.ToC_node_builder import TOCHierarchicalNodeBuilder as DefaultNodeBuilder
    from src.phase2_parsing.node_builders.node_builder import FinLensNode
except ImportError as e:
    logger.critical(f"Failed to import necessary FinLens components: {e}",
                    exc_info=True)
    sys.exit(1)
# -----------------------------------------


class ProcessorService:
    """
    Orchestrates the Phase 2 parsing process using sec-parser.
    """

    def __init__(self, settings: Optional[AppSettings] = None):
        logger.info("Initializing ProcessorService...")
        try:
            self.settings = settings or get_settings()

            # --- Initialize sec-parser ---
            self.sec_parser_instance: Optional[SecParser] = None
            if PARSER_CHOICE == "sec_parser":
                if not SEC_PARSER_AVAILABLE:
                    # This should already cause an exit above, but double-check
                    raise RuntimeError(
                        "sec-parser is selected but not available.")
                self.sec_parser_instance = SecParser()
                logger.info("Initialized SecParser instance.")
            # -----------------------------

            # --- Initialize common components ---
            self.metadata_extractor = MetadataExtractor()
            self.toc_extractor = ToCExtractor(
            )  # May need changes for sec-parser
            self.node_builder = DefaultNodeBuilder(
            )  # Will need changes for sec-parser
            # ----------------------------------

            self.base_data_path = Path(self.settings.pipeline.data_path)
            self.html_fixture_path = Path(
                __file__).resolve().parent.parent.parent / "tests" / "fixtures"
            self.output_nodes_path = self.base_data_path / "nodes_json"
            # Removed temp_pdf_path
            self.output_nodes_path.mkdir(parents=True, exist_ok=True)

            logger.info(f"HTML Fixtures expected in: {self.html_fixture_path}")
            logger.info(f"Node JSON output path: {self.output_nodes_path}")
            logger.info("ProcessorService initialized successfully.")

        except Exception as e:
            logger.critical(f"Failed to initialize ProcessorService: {e}",
                            exc_info=True)
            raise RuntimeError(
                f"ProcessorService initialization failed: {e}") from e

    def _get_filing_metadata_from_filename(self,
                                           filename: str) -> Dict[str, Any]:
        """Parses metadata from various structured filename formats using regex."""
        # (Keep this function as implemented previously)
        base_name = filename.rsplit('.', 1)[0]
        metadata = {
            "cik": "unknown",
            "ticker": "unknown",
            "filing_date": "unknown",
            "form_type": "unknown",
            "accession_number": base_name,
            "filename_base": base_name
        }
        pattern1 = re.match(r"^(\d+)_(\d{4}-\d{2}-\d{2})_([^_]+)_(.+)$",
                            base_name)
        if pattern1:
            metadata.update({
                "cik": pattern1.group(1),
                "filing_date": pattern1.group(2),
                "form_type": pattern1.group(3).upper(),
                "accession_number": pattern1.group(4),
            })
            return metadata
        pattern2 = re.match(
            r"^([a-zA-Z0-9]+)-([a-zA-Z0-9kK-]+)_(\d{4}-?\d{2}-?\d{2})$",
            base_name)
        if pattern2:
            metadata.update({
                "ticker":
                pattern2.group(1).upper(),
                "form_type":
                pattern2.group(2).upper().replace('-', ''),
                "filing_date":
                pattern2.group(3).replace('-', ''),
            })
            return metadata
        pattern3 = re.match(r"^([a-zA-Z0-9]+)-(\d{4}-?\d{2}-?\d{2})$",
                            base_name)
        if pattern3:
            metadata.update({
                "ticker": pattern3.group(1).upper(),
                "filing_date": pattern3.group(2).replace('-', ''),
            })
            if '10k' in metadata['ticker'].lower():
                metadata['form_type'] = '10K'
            elif '10q' in metadata['ticker'].lower():
                metadata['form_type'] = '10Q'
            return metadata
        logger.warning(
            f"Could not parse metadata from filename using known patterns: {filename}. Using defaults."
        )
        return metadata

    def _write_nodes_to_json(self, nodes: List[FinLensNode],
                             output_path: Path):
        """Serializes the list of FinLensNode objects to a JSON file."""
        logger.debug(f"Writing {len(nodes)} nodes to {output_path}...")
        try:
            nodes_as_dicts = [
                node.model_dump(mode='json')
                if hasattr(node, 'model_dump') else node.__dict__
                for node in nodes
            ]
            with open(output_path, 'w', encoding='utf-8') as f:
                json.dump(nodes_as_dicts, f, indent=2, ensure_ascii=False)
            logger.debug(f"Successfully wrote nodes to {output_path}")
        except Exception as e:
            logger.error(f"Failed to write nodes JSON to {output_path}: {e}",
                         exc_info=True)

    def _process_single_filing(
        self, filing_info: Dict[str, Any]
    ) -> Tuple[str, Optional[List[FinLensNode]], Optional[FinLensNode], bool]:
        """
        Processes a single HTML filing using sec-parser.
        """
        identifier = filing_info.get("filename_base", "unknown_file")
        html_path = filing_info.get("html_path")  # Original HTML path
        output_json_path = filing_info.get("output_json_path")
        error_occurred = False

        if not html_path or not isinstance(html_path,
                                           Path) or not html_path.exists():
            logger.error(
                f"Invalid or missing HTML path for {identifier}: {html_path}")
            return identifier, None, None, True

        logger.info(
            f"--- Processing filing: {identifier} from {html_path.name} ---")

        # --- Select Parser ---
        parser_output: Any = None  # Use Any for now, refine later
        raw_html_content: Optional[str] = None

        try:
            # --- Read HTML Content ---
            encodings_to_try = [
                'utf-8', 'latin-1', 'iso-8859-1', 'windows-1252'
            ]
            for enc in encodings_to_try:
                try:
                    with open(html_path, "r", encoding=enc) as f:
                        raw_html_content = f.read()
                    logger.debug(
                        f"Successfully read {html_path.name} with encoding {enc}"
                    )
                    break
                except UnicodeDecodeError:
                    logger.debug(
                        f"Encoding {enc} failed for {html_path.name}, trying next..."
                    )
                except Exception as read_err:
                    logger.error(
                        f"Failed to read {html_path} for parsing: {read_err}")
                    return identifier, None, None, True  # Cannot proceed

            if raw_html_content is None:
                logger.error(
                    f"Could not decode HTML file {html_path.name} with attempted encodings."
                )
                return identifier, None, None, True
            # -------------------------

            # --- Use sec-parser ---
            if PARSER_CHOICE == "sec_parser":
                if not self.sec_parser_instance:
                    logger.critical("sec-parser instance not initialized.")
                    return identifier, None, None, True

                logger.info(
                    f"Attempting parse for {html_path.name} using sec-parser..."
                )
                try:
                    # Pass the HTML string content to sec-parser
                    semantic_elements: List[
                        AbstractSemanticElement] = self.sec_parser_instance.parse(
                            raw_html_content)
                    parser_output = semantic_elements  # Store the list of elements
                    logger.info(
                        f"sec-parser successfully processed {html_path.name} ({len(semantic_elements)} elements found)."
                    )
                except Exception as sec_parse_err:
                    logger.error(
                        f"sec-parser failed for {html_path.name}: {sec_parse_err}",
                        exc_info=True)
                    return identifier, None, None, True  # Error during parsing
            # ----------------------

            # --- Placeholder for Docling/Hybrid Logic (Currently Unused) ---
            elif PARSER_CHOICE == "docling":
                logger.error(
                    "Docling parser path is selected but not fully implemented in this version."
                )
                # Add back DoclingWrapper call, potentially with PDF conversion if needed for this path
                # parser_output = self.docling_wrapper_instance.parse(...)
                return identifier, None, None, True  # Not implemented
            elif PARSER_CHOICE == "hybrid":
                logger.error(
                    "Hybrid parser path is selected but not implemented.")
                # Implement logic combining sec-parser and potentially Docling
                return identifier, None, None, True  # Not implemented
            else:
                logger.error(f"Invalid PARSER_CHOICE: {PARSER_CHOICE}")
                return identifier, None, None, True
            # -------------------------------------------------------------

            if parser_output is None:
                logger.error(
                    f"Parsing step failed to produce output for {identifier}")
                return identifier, None, None, True  # Error

            # --- Downstream Processing ---
            # **IMPORTANT**: These components need modification to handle 'parser_output'
            # which is now potentially a list of sec-parser elements.

            # 2. Extract Metadata & ToC
            doc_meta = {  # Start with filename metadata
                "accession_number": filing_info.get("accession_number"),
                "cik": filing_info.get("cik"),
                "ticker": filing_info.get("ticker"),
                "form_type": filing_info.get("form_type"),
                "filing_date": filing_info.get("filing_date"),
                "filename_base": filing_info.get("filename_base"),
                "toc": []
            }
            try:
                # MetadataExtractor might need adaptation
                # It previously expected a DoclingDocument and raw HTML.
                # Now it gets sec-parser elements and raw HTML.
                extracted_meta = self.metadata_extractor.extract(
                    parser_output, raw_html_content)  # Needs update
                doc_meta.update({k: v for k, v in extracted_meta.items() if v})

                # ToCExtractor might still work on raw HTML, or could be adapted
                # to use sec-parser's potential ToC elements if available.
                if raw_html_content:
                    toc_list = self.toc_extractor.extract_from_html(
                        raw_html_content)
                    doc_meta['toc'] = toc_list
                else:
                    logger.warning(
                        f"Skipping ToC extraction for {identifier} as raw HTML could not be read."
                    )

            except Exception as e:
                logger.error(
                    f"Metadata or ToC extraction failed for {identifier}: {e}",
                    exc_info=True)
                # Decide if this is a critical error or if processing can continue

            # 3. Build Node Tree
            nodes: Optional[List[FinLensNode]] = None
            root_node: Optional[FinLensNode] = None
            try:
                # Node Builder *definitely* needs adaptation.
                # It previously expected a DoclingDocument and metadata (with ToC).
                # Now it gets sec-parser elements and metadata (with ToC).
                # It needs to traverse the sec-parser elements to build the FinLensNode tree.
                logger.warning(
                    f"Node Builder needs adaptation for sec-parser output for {identifier}."
                )
                # This call will likely fail or produce incorrect results without updates:
                nodes, root_node = self.node_builder.build_tree(
                    parser_output, doc_meta)  # Needs significant update
            except Exception as e:
                logger.error(
                    f"Node tree building failed for {identifier} (likely needs adaptation for sec-parser): {e}",
                    exc_info=True)
                error_occurred = True  # Mark error

            if not error_occurred and (not nodes or not root_node):
                logger.warning(
                    f"No nodes/root generated for {identifier} by the node builder (check adaptation)."
                )
                # Treat as warning for now

            # 4. Write nodes to JSON
            if not error_occurred and nodes and root_node:
                self._write_nodes_to_json(nodes, output_json_path)
                logger.info(
                    f"--- Successfully processed (structure only): {identifier} -> {output_json_path.name} ({len(nodes)} nodes) ---"
                )
            elif not error_occurred:
                logger.warning(
                    f"Processing technically succeeded for {identifier}, but no nodes were generated."
                )
            # else: Error already logged

        except Exception as proc_err:
            logger.error(
                f"Unexpected error processing {identifier}: {proc_err}",
                exc_info=True)
            error_occurred = True

        # Determine final status
        if not error_occurred and 'nodes' in locals() and nodes:
            return identifier, nodes, root_node, False  # Success (but content likely missing)
        elif not error_occurred:
            return identifier, None, None, False  # Warning (no nodes)
        else:
            return identifier, None, None, True  # Error

    def run_processing(self, html_files: List[Path]):
        """ Processes a list of HTML files sequentially using the configured parser. """
        if not html_files:
            logger.warning("No input files provided for processing.")
            return

        tasks_info = []
        processed_bases = set()
        for file_path in html_files:  # Use correct variable name
            if not file_path.is_file():
                logger.warning(f"Skipping non-existent file: {file_path}")
                continue

            metadata = self._get_filing_metadata_from_filename(file_path.name)
            filename_base = metadata['filename_base']
            if filename_base in processed_bases:
                logger.warning(
                    f"Skipping duplicate file based on filename base: {file_path.name}"
                )
                continue
            processed_bases.add(filename_base)

            output_json_path = self.output_nodes_path / f"{filename_base}_nodes.json"
            tasks_info.append({
                **metadata,
                "html_path": file_path,  # Pass original HTML path
                "output_json_path": output_json_path
            })

        if not tasks_info:
            logger.warning(
                "No valid, unique tasks prepared after checking input files.")
            return

        num_files = len(tasks_info)
        logger.info(
            f"Starting sequential processing for {num_files} files using '{PARSER_CHOICE}' parser..."
        )

        results_summary: Dict[str, Dict[str, Any]] = {}
        success_count = 0
        warning_count = 0
        error_count = 0
        start_time = time.time()

        for i, task_info in enumerate(tasks_info):
            identifier = task_info.get("filename_base")
            try:
                id_res, nodes_res, root_res, error_flag = self._process_single_filing(
                    task_info)
                result_key = id_res

                if error_flag:
                    error_count += 1
                    results_summary[result_key] = {
                        "status": "Error",
                        "details": "Processing failed (check logs)"
                    }
                elif nodes_res is not None and root_res is not None:
                    success_count += 1
                    results_summary[result_key] = {
                        "status": "OK",
                        "details":
                        f"{len(nodes_res)} nodes (Adaptation needed)"
                    }  # Note adaptation needed
                else:
                    warning_count += 1
                    results_summary[result_key] = {
                        "status": "Warn",
                        "details": "No nodes (Adaptation needed)"
                    }  # Note adaptation needed

            except Exception as loop_exc:
                error_count += 1
                logger.error(
                    f"Critical error during loop execution for {identifier}: {loop_exc}",
                    exc_info=True)
                results_summary[identifier] = {
                    "status": "Error",
                    "details": f"Loop exception: {type(loop_exc).__name__}"
                }

            logger.info(f"Progress: {i+1}/{num_files} files processed...")

        end_time = time.time()
        duration = end_time - start_time
        logger.info(
            f"Finished processing {num_files} files sequentially in {duration:.2f} seconds."
        )

        # --- Print Summary ---
        print("\n" + "--- Processing Summary ---")
        print(f"Total Files Submitted: {num_files}")
        print(
            f"  Success (Nodes Generated - Needs Adaptation): {success_count}")
        print(
            f"  Warnings (No Nodes/Root - Needs Adaptation): {warning_count}")
        print(f"  Errors (Exceptions/Failures): {error_count}")
        print("--- Individual Results ---")
        sorted_results = sorted(results_summary.items())
        for identifier, summary in sorted_results:
            print(f"  {identifier}: {summary['status']}: {summary['details']}")
        print("--------------------------")


# --- Example Usage ---
if __name__ == "__main__":
    logger.info("Running ProcessorService directly...")

    script_dir = Path(__file__).parent
    fixture_dir = script_dir.parent.parent.parent / "tests" / "fixtures"

    file_categories = {
        "recent_finance_10k_by_name": ["jpm-20241231.htm"],
        "recent_tech_10k_by_name": [
            "aapl-20240928.htm", "aapl-20230930.htm", "goog-20241231.htm",
            "goog-20231231.htm", "goog-20221231.htm"
        ],
        "recent_auto_10k_by_name":
        ["tsla-20241231.htm", "tsla-20231231.htm", "tsla-20221231.htm"],
        "recent_energy_10k_by_name":
        ["xom-20241231.htm", "xom-20231231.htm", "xom-20221231.htm"],
        "recent_amendment_by_name":
        ["tsla-10ka_20211231.htm", "tsla-10ka_20201231.htm"],
    }

    all_files_to_process: List[Path] = []
    for category, filenames in file_categories.items():
        prepared_count = 0
        for filename in filenames:
            file_path = fixture_dir / filename
            if file_path.exists():
                all_files_to_process.append(file_path)
                prepared_count += 1
            else:
                logger.warning(f"Fixture file not found: {file_path}")
        if prepared_count < len(filenames):
            logger.warning(
                f"Could only prepare {prepared_count}/{len(filenames)} filings for category {category}"
            )

    if not all_files_to_process:
        logger.error(
            f"No valid fixture files found in {fixture_dir}. Exiting.")
        sys.exit(1)

    try:
        processor = ProcessorService()
        processor.run_processing(all_files_to_process)  # Run sequentially
    except Exception as main_err:
        logger.critical(f"ProcessorService failed to run: {main_err}",
                        exc_info=True)
        sys.exit(1)

    logger.info("ProcessorService run complete.")
