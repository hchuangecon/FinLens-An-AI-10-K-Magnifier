# Test script comparing Docling and Edgar table parsing
# Assumes 'edgar' package is installed and allows specified imports
# FINAL: Uses static call for Docling based on user's original script.

import logging
import sys
import time
from io import StringIO
from pathlib import Path
from typing import Optional, List, Union, Dict, Any, TYPE_CHECKING

import re
import textwrap
import warnings
from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from functools import cached_property, lru_cache

# --- Dependency Imports ---
BS4_AVAILABLE = False
try:
    from bs4 import BeautifulSoup, Tag, NavigableString, Comment, XMLParsedAsHTMLWarning
    BS4_AVAILABLE = True
    warnings.filterwarnings("ignore", category=XMLParsedAsHTMLWarning)
except ImportError:
    print(
        "ERROR: BeautifulSoup (bs4) not found. Install: pip install beautifulsoup4 lxml"
    )
    sys.exit(1)

DOCLING_AVAILABLE = False
try:
    # Import names needed for static call and type checking
    from docling.backend.html_backend import HTMLDocumentBackend
    from docling_core.types.doc import TableData, TableCell as DoclingTableCellCls
    DOCLING_AVAILABLE = True
    print("Successfully imported Docling components.")
except ImportError as e:
    print(f"WARNING: Failed to import Docling components: {e}")
    DOCLING_AVAILABLE = False

EDGAR_AVAILABLE = False
RICH_AVAILABLE = False
try:
    # Direct imports for Edgar
    from edgar.files.styles import StyleInfo, Width, parse_style
    from edgar.files.html import BaseNode, TableNode, TableCell, TableRow, SECHTMLParser, Document
    from edgar.files.tables import TableProcessor, ProcessedTable, is_number, ColumnOptimizer
    try:
        from rich.table import Table as RichTable
        from rich import box
        RICH_AVAILABLE = True
    except ImportError:
        print("WARNING: rich library not found.")
        RICH_AVAILABLE = False
    EDGAR_AVAILABLE = True
    if 'SECHTMLParser' not in locals() or 'Document' not in locals():
        print(
            "ERROR: Critical SECHTMLParser or Document class missing after import attempt."
        )
        EDGAR_AVAILABLE = False
    else:
        print("Successfully imported Edgar components (incl. SECHTMLParser).")
except ImportError as e:
    print(f"ERROR: Failed to import Edgar components: {e}")
    EDGAR_AVAILABLE = False

# --- Type Hinting Setup ---
if TYPE_CHECKING:
    from bs4 import Tag as Bs4TagType
    if DOCLING_AVAILABLE:
        from docling_core.types.doc import TableData as HintTableData
    if EDGAR_AVAILABLE:
        from edgar.files.html import Document as HintDocument, TableNode as HintTableNode
        from edgar.files.tables import ProcessedTable as HintProcessedTable

# --- Logging Setup ---
logging.basicConfig(level=logging.INFO,
                    format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)


# === Helper Functions (Unchanged) ===
def _convert_docling_tabledata_to_markdown(table_data: Optional[Any]) -> str:
    # ... (Implementation from response #41 - unchanged) ...
    if not DOCLING_AVAILABLE: return "```\nDocling N/A.\n```"
    if table_data is None: return "```\nDocling TableData is None.\n```"
    if not hasattr(table_data, 'rows') or not table_data.rows:
        return "```\nDocling TableData missing 'rows' or empty.\n```"
    if not all(hasattr(row, 'cells') for row in table_data.rows):
        return "```\nDocling TableData rows missing 'cells'.\n```"
    max_cols = 0
    for row in table_data.rows:
        current_cols = 0
        for cell in row.cells:
            current_cols += getattr(cell, 'col_span', 1)
        max_cols = max(max_cols, current_cols)
    if max_cols == 0:
        return "```\nDocling TableData has rows but no columns/spans.\n```"
    markdown_lines = []
    header_separator = "| " + " | ".join(["---"] * max_cols) + " |"
    has_separator = False
    for r_idx, row in enumerate(table_data.rows):
        md_row_cells = []
        row_len = 0
        for cell in row.cells:
            cell_text = getattr(cell, 'text', '')
            cell_text = cell_text.replace('\n', ' ').replace('|',
                                                             '\\|').strip()
            md_row_cells.append(cell_text)
            col_span = getattr(cell, 'col_span', 1)
            row_len += col_span
            for _ in range(col_span - 1):
                md_row_cells.append("")
        md_row_cells = md_row_cells[:max_cols]
        md_row_cells.extend([""] * (max_cols - len(md_row_cells)))
        markdown_lines.append("| " + " | ".join(md_row_cells) + " |")
        if r_idx == 0 and len(table_data.rows) > 1:
            markdown_lines.append(header_separator)
            has_separator = True
    if len(table_data.rows) == 1:
        markdown_lines.append(header_separator)
        has_separator = True
    if not has_separator and markdown_lines:
        markdown_lines.insert(1, header_separator)
    return "```markdown\n" + "\n".join(markdown_lines) + "\n```"


def convert_edgar_processed_to_markdown(processed_table: Optional[Any]) -> str:
    # ... (Implementation from response #41 - unchanged) ...
    if not EDGAR_AVAILABLE: return "```\nEdgar N/A.\n```"
    if processed_table is None:
        return "```\nEdgar ProcessedTable is None.\n```"
    if not hasattr(processed_table, 'data_rows') or not hasattr(
            processed_table, 'headers') or not hasattr(processed_table,
                                                       'column_alignments'):
        logger.warning("ProcessedTable object missing expected attributes.")
        return "```\nInvalid ProcessedTable structure.\n```"
    if not processed_table.data_rows and not processed_table.headers:
        return "```\nEdgar ProcessedTable is empty.\n```"
    markdown_lines = []
    col_count = 0
    if processed_table.headers:
        col_count = len(processed_table.headers)
        cleaned_headers = [
            str(h).replace('\n', ' ').replace('|', '\\|').strip()
            for h in processed_table.headers
        ]
        markdown_lines.append("| " + " | ".join(cleaned_headers) + " |")
        sep_parts = []
        if processed_table.column_alignments and len(
                processed_table.column_alignments) == col_count:
            for align in processed_table.column_alignments:
                if align == 'right': sep_parts.append('---:')
                elif align == 'center': sep_parts.append(':---:')
                else: sep_parts.append(':---')
        else: sep_parts = ['---'] * col_count
        markdown_lines.append("|" + "|".join(sep_parts) + "|")
    if processed_table.data_rows:
        if col_count == 0:
            col_count = len(processed_table.data_rows[0]
                            ) if processed_table.data_rows else 0
        for row in processed_table.data_rows:
            cleaned_row = [
                str(cell).replace('\n', ' ').replace('|', '\\|').strip()
                for cell in row
            ]
            if len(cleaned_row) < col_count:
                cleaned_row.extend([""] * (col_count - len(cleaned_row)))
            elif len(cleaned_row) > col_count:
                cleaned_row = cleaned_row[:col_count]
            markdown_lines.append("| " + " | ".join(cleaned_row) + " |")
    if not processed_table.headers and processed_table.data_rows and col_count > 0:
        markdown_lines.insert(
            0,
            "| " + " | ".join([f"Col {i+1}" for i in range(col_count)]) + " |")
        markdown_lines.insert(1, "| " + " | ".join(["---"] * col_count) + " |")
    return "```markdown\n" + "\n".join(markdown_lines) + "\n```"


def _build_edgar_tablenode_from_tag(table_tag: Any) -> Optional[Any]:
    # ... (Implementation from response #41 - unchanged) ...
    if not BS4_AVAILABLE or not EDGAR_AVAILABLE: return None
    if 'TableCell' not in globals() or 'TableRow' not in globals(
    ) or 'TableNode' not in globals() or 'parse_style' not in globals(
    ) or 'StyleInfo' not in globals():
        return None
    if not table_tag or not hasattr(table_tag, 'find_all'): return None
    rows_data: List['TableRow'] = []
    table_style = parse_style(table_tag.get('style', ''))
    for tr_tag in table_tag.find_all('tr', recursive=False):
        cells_data: List['TableCell'] = []
        th_found_in_row = False
        for td_tag in tr_tag.find_all(['td', 'th'], recursive=False):
            if td_tag.name == 'th': th_found_in_row = True
            try:
                current_cell_tag: Tag = td_tag
                cell_text = " ".join(
                    current_cell_tag.get_text(strip=True).split())
                colspan = 1
                rowspan = 1
                try:
                    colspan = int(current_cell_tag.get('colspan', '1'))
                except (ValueError, TypeError):
                    pass
                try:
                    rowspan = int(current_cell_tag.get('rowspan', '1'))
                except (ValueError, TypeError):
                    pass
                colspan = max(1, colspan)
                rowspan = max(1, rowspan)
                align = current_cell_tag.get('align', 'left')
                cell_style = parse_style(current_cell_tag.get('style', ''))
                if cell_style and cell_style.text_align:
                    align = cell_style.text_align
                cells_data.append(
                    TableCell(content=cell_text,
                              colspan=colspan,
                              rowspan=rowspan,
                              align=align))
            except Exception as cell_e:
                logger.error(f"Error processing cell: {cell_e}",
                             exc_info=False)
                cells_data.append(
                    TableCell(content="[ERROR]", colspan=1, rowspan=1))
        if cells_data:
            rows_data.append(
                TableRow(cells=cells_data, is_header=th_found_in_row))
    if rows_data:
        return TableNode(content=rows_data, style=table_style)
    else:
        return None


# === Main Comparison Logic ===
def run_comparison(html_filepath: str, output_filepath: str,
                   docling_available_flag: bool, edgar_available_flag: bool):
    """
    Runs the comparison. Edgar: Parses full document. Docling: Uses find_all + Static Call.
    """
    # (Initial logging and checks remain the same)
    logger.info(
        "Starting table parsing comparison (Edgar Full Parse vs Docling find_all)..."
    )
    logger.info(f"HTML Input: {html_filepath}")
    logger.info(f"Markdown Output: {output_filepath}")
    if not BS4_AVAILABLE:
        logger.error("BeautifulSoup not available.")
        return
    if not docling_available_flag and not edgar_available_flag:
        logger.error("Neither Docling nor Edgar available.")
        return
    if not docling_available_flag:
        logger.warning("Docling not available. Skipping Docling.")
    if not edgar_available_flag:
        logger.warning("Edgar components not available. Skipping Edgar.")
    html_path = Path(html_filepath)
    output_path = Path(output_filepath)
    if not html_path.is_file():
        logger.error(f"HTML input file not found: {html_path}")
        return
    try:
        html_content = html_path.read_text(encoding="utf-8")
        logger.info(f"Read {len(html_content)} bytes from {html_path}")
    except Exception as e:
        logger.error(f"Failed to read HTML file: {e}", exc_info=True)
        return

    soup = BeautifulSoup(html_content, "lxml")

    # --- Edgar: Full Document Parse (Unchanged) ---
    edgar_full_parse_time = 0.0
    edgar_table_nodes: List[Any] = []
    if edgar_available_flag:
        logger.info("Starting Edgar full document parse...")
        # (Full parse logic remains the same as response #41)
        try:
            start_time = time.time()
            root_element = soup.find('html') or soup.find('body') or soup
            if root_element and 'SECHTMLParser' in globals(
            ) and 'Document' in globals():
                parser = SECHTMLParser(root_element)
                edgar_doc = parser.parse()
                if edgar_doc and hasattr(edgar_doc, 'nodes'):
                    if 'TableNode' in globals():
                        edgar_table_nodes = [
                            node for node in edgar_doc.nodes
                            if isinstance(node, TableNode)
                        ]
                    else:
                        logger.error("Edgar TableNode class unavailable.")
                elif edgar_doc is None:
                    logger.error("Edgar parser.parse() returned None.")
            else:
                logger.error(
                    "Could not find root element or SECHTMLParser/Document unavailable."
                )
            edgar_full_parse_time = time.time() - start_time
            logger.info(
                f"Edgar full parse finished in {edgar_full_parse_time:.4f}s. Found {len(edgar_table_nodes)} TableNodes."
            )
        except Exception as e:
            logger.error(f"Edgar full document parsing failed: {e}",
                         exc_info=True)
            edgar_available_flag = False

    # --- Edgar: Process Identified Tables (Unchanged) ---
    edgar_results = []
    edgar_success_count = 0
    total_edgar_processing_time = 0.0
    if edgar_available_flag and edgar_table_nodes:
        logger.info(
            f"Processing {len(edgar_table_nodes)} tables found by Edgar parser..."
        )
        # (Processing loop remains the same as response #41)
        for i, table_node in enumerate(edgar_table_nodes):
            table_num = i + 1
            node_results = []
            node_results.append(
                f"## Edgar Table Node {table_num} (from full parse)\n")
            edgar_parsed_md = "```\nError processing node.\n```"
            edgar_time_taken = 0.0
            processed_table = None
            try:
                start_time = time.time()
                if table_node and 'TableProcessor' in globals():
                    if hasattr(table_node, '_processed'):
                        processed_table = table_node._processed
                    else:
                        processed_table = TableProcessor.process_table(
                            table_node)
                edgar_parsed_md = convert_edgar_processed_to_markdown(
                    processed_table)
                edgar_time_taken = time.time() - start_time
                total_edgar_processing_time += edgar_time_taken
                if processed_table is not None: edgar_success_count += 1
            except Exception as e:
                logger.warning(
                    f"Edgar processing failed for TableNode {table_num}: {e}",
                    exc_info=False)
                edgar_parsed_md = f"```error\nEdgar processing failed: {e}\n```"
                edgar_time_taken = time.time() - start_time
                total_edgar_processing_time += edgar_time_taken
            node_results.append(
                f"**Edgar Time (Process+Convert):** {edgar_time_taken:.4f}s\n")
            node_results.append(
                "### Edgar Parsed (`TableProcessor` -> Markdown):\n")
            node_results.append(edgar_parsed_md + "\n\n---\n")
            edgar_results.extend(node_results)
        logger.info("Finished processing Edgar tables.")

    # --- Docling: Process Tables via find_all ---
    docling_results = []
    docling_success_count = 0
    total_docling_time = 0.0
    docling_processed_count = 0
    # *** NO Docling Backend Instantiation Here ***
    if docling_available_flag:  # Use passed flag
        logger.info(
            "Processing tables found by find_all() with Docling (Static Call)..."
        )
        soup_tables = soup.find_all("table")
        docling_processed_count = len(soup_tables)
        for i, table_tag in enumerate(soup_tables):
            table_num = i + 1
            node_results = []
            node_results.append(
                f"## Docling Table {table_num} (from find_all)\n")
            docling_parsed_md = "```\nError processing tag.\n```"
            docling_time_taken = 0.0
            try:
                start_time = time.time()
                # *** Use STATIC CALL based on original script ***
                if 'HTMLDocumentBackend' in globals(
                ):  # Check class was imported
                    # Call parse_table_data directly on the class
                    docling_table_data = HTMLDocumentBackend.parse_table_data(
                        element=table_tag)
                else:
                    raise RuntimeError(
                        "HTMLDocumentBackend class not available.")

                docling_parsed_md = _convert_docling_tabledata_to_markdown(
                    docling_table_data)
                docling_time_taken = time.time() - start_time
                total_docling_time += docling_time_taken
                docling_success_count += 1
            except AttributeError as ae:  # Catch if parse_table_data is not static/classmethod
                logger.warning(
                    f"Docling static call failed for Table {table_num}: {ae}. Needs instance or different method.",
                    exc_info=False)
                docling_parsed_md = f"```error\nDocling static call failed: {ae}\n```"
                docling_time_taken = time.time() - start_time
                total_docling_time += docling_time_taken
            except Exception as e:  # Catch other potential errors
                logger.warning(
                    f"Docling parsing failed for Table {table_num}: {e}",
                    exc_info=False)
                docling_parsed_md = f"```error\nDocling parsing failed: {e}\n```"
                docling_time_taken = time.time() - start_time
                total_docling_time += docling_time_taken

            node_results.append(
                f"**Docling Time (Parse+Convert):** {docling_time_taken:.4f}s\n"
            )
            node_results.append(
                "### Docling Parsed (`parse_table_data` -> Markdown):\n")
            node_results.append(docling_parsed_md + "\n\n---\n")
            docling_results.extend(node_results)
        logger.info("Finished processing Docling tables.")

    # --- Calculate Summary Stats & Write Results ---
    # (Summary/File writing logic remains the same as response #41)
    avg_docling_time = (total_docling_time / docling_processed_count
                        if docling_processed_count > 0 else 0)
    avg_edgar_processing_time = (total_edgar_processing_time /
                                 len(edgar_table_nodes)
                                 if edgar_table_nodes else 0)
    logger.info(f"Writing comparison results to {output_path}...")
    # (File writing remains the same)
    try:
        with open(output_path, "w", encoding="utf-8") as f:
            f.write(f"# Table Parsing Comparison: {html_path.name}\n\n")
            f.write(f"Comparing Edgar (Full Parse) vs Docling (find_all)\n\n")
            f.write("## Performance Summary:\n\n")
            if edgar_available_flag:  # Use final state of flag
                f.write(f"**Edgar (Full Document Parse):**\n")
                f.write(
                    f"* Initial Full Parse Time: {edgar_full_parse_time:.4f}s\n"
                )
                f.write(
                    f"* Tables Identified (TableNodes): {len(edgar_table_nodes)}\n"
                )
                f.write(
                    f"* Tables Processed Successfully: {edgar_success_count}/{len(edgar_table_nodes)}\n"
                )
                f.write(
                    f"* Total Table Processing Time (Process+Convert): {total_edgar_processing_time:.4f}s\n"
                )
                f.write(
                    f"* Average Table Processing Time: {avg_edgar_processing_time:.4f}s\n\n"
                )
            else:
                f.write(f"* **Edgar:** Skipped\n\n")
            if docling_available_flag:  # Use final state of flag
                f.write(f"**Docling (find_all Parse):**\n")
                f.write(
                    f"* Tables Found (find_all): {docling_processed_count}\n")
                f.write(
                    f"* Tables Processed Successfully: {docling_success_count}/{docling_processed_count}\n"
                )
                f.write(
                    f"* Total Table Processing Time (Parse+Convert): {total_docling_time:.4f}s\n"
                )
                f.write(
                    f"* Average Table Processing Time: {avg_docling_time:.4f}s\n"
                )
            else:
                f.write(f"* **Docling:** Skipped\n")
            f.write("\n---\n\n")
            f.write("## Edgar Detailed Results (Tables from Full Parse):\n\n")
            if edgar_available_flag and edgar_results:
                f.write("\n".join(edgar_results))
            elif edgar_available_flag:
                f.write("No tables identified or processed by Edgar.\n")
            else:
                f.write("Edgar processing skipped.\n")
            f.write("\n---\n\n")
            f.write("## Docling Detailed Results (Tables from find_all):\n\n")
            if docling_available_flag and docling_results:
                f.write("\n".join(docling_results))
            elif docling_available_flag:
                f.write("No tables processed by Docling.\n")
            else:
                f.write("Docling processing skipped.\n")
    except Exception as e:
        logger.error(f"Failed to write results file: {e}", exc_info=True)

    logger.info("Comparison test finished.")
    print(f"\nPerformance Summary:")
    if edgar_available_flag:
        print(f"* Edgar Full Parse Time: {edgar_full_parse_time:.4f}s")
        print(
            f"* Edgar Tables Found/Processed: {edgar_success_count}/{len(edgar_table_nodes)}"
        )
        print(f"* Edgar Avg Table Proc Time: {avg_edgar_processing_time:.4f}s")
    if docling_available_flag:
        print(
            f"* Docling Tables Found/Processed: {docling_success_count}/{docling_processed_count}"
        )
        print(f"* Docling Avg Table Proc Time: {avg_docling_time:.4f}s")
    print(f"\nResults saved to: {output_path}")


# === Script Execution ===
if __name__ == "__main__":
    script_dir = Path(
        __file__).parent if "__file__" in locals() else Path.cwd()
    html_file_to_test = script_dir / "fixtures" / "aapl-20240928.htm"
    output_dir = script_dir / "output"
    output_dir.mkdir(parents=True, exist_ok=True)
    output_file = output_dir / f"{html_file_to_test.stem}_edgar_full_vs_docling_ORIGINAL_DOCLING.md"  # New name

    if not html_file_to_test.is_file():
        print(f"ERROR: Specified HTML file not found: {html_file_to_test}")
        sys.exit(1)

    # Pass the global availability flags
    run_comparison(str(html_file_to_test), str(output_file), DOCLING_AVAILABLE,
                   EDGAR_AVAILABLE)
