# src/phase1_extraction/parsers/index.py

import logging
import re
from io import StringIO  # To handle string content like a file
from datetime import datetime
from typing import Any, Dict, List, Optional, Set, Tuple

from .base import AbstractParser
from src.core.exceptions import ParsingError, IndexParsingError
from src.config.settings import AppSettings

logger = logging.getLogger(__name__)

# Define type alias for clarity, list of filing dictionaries
ParseResult = List[Dict[str, Any]]


class IndexParser(AbstractParser):
    """
    Parses SEC Master Index files (daily or quarterly) which are typically
    pipe-delimited, extracting filing metadata. Handles variations in header.
    """

    def __init__(self, settings: AppSettings):
        """Initializes the parser with necessary configurations."""
        super().__init__(settings)  # Call base class __init__

    # Overriding the parse method from the base class
    def parse(
            self,
            input_source: str,  # Expecting string content from downloader
            source_description: Optional[
                str] = None,  # e.g., date or year/qtr for logging
            target_forms: Optional[Set[
                str]] = None,  # Optional set of upper-case forms to filter for
            *args,
            **kwargs) -> ParseResult:
        """
        Parses the text content of a pipe-delimited master index file.

        Args:
            input_source: The raw string content of the index file.
            source_description: Optional descriptive name for the source (e.g., "2023-Q1", "2024-04-22")
                                used for logging context.
            target_forms: Optional set of upper-case form types to filter for. If None,
                          all valid filing lines are returned. Example: {'10-K', '10-K/A'}.

        Returns:
            A list of dictionaries, each representing a filing found that matches
            the target_forms (if provided). Returns empty list if no relevant
            filings are found or on parsing errors that prevent extraction.

        Raises:
            IndexParsingError: If fundamental parsing fails (e.g., cannot find header separator).
        """
        filings_found: ParseResult = []
        # Use StringIO to easily iterate lines from the input string
        # Handle potential different line endings robustly
        content_io = StringIO(
            input_source.replace('\r\n', '\n').replace('\r', '\n'))
        header_skipped = False
        header_found = False  # Flag to track if we've seen the header line itself
        line_count = 0
        errors_in_source = 0
        source_id = source_description or 'Unknown Source'  # Use a short ID for logs

        logger.info(f"Starting parsing of index source: {source_id}")

        for line_num, line in enumerate(content_io):
            line_count += 1
            line = line.strip()

            # --- Header Skipping Logic ---
            # Skip descriptive lines until we find the header line, then skip the separator.
            if not header_skipped:
                line_content = line.strip()
                # Check for EITHER known header version using exact match
                is_header_line = (
                    line_content
                    == 'CIK|Company Name|Form Type|Date Filed|Filename'
                    or line_content
                    == 'CIK|Company Name|Form Type|Date Filed|File Name')

                if is_header_line:
                    header_found = True
                    # Don't skip yet, wait for the separator line below it
                elif header_found and line_content.startswith('---'):
                    # Found the separator line AFTER the header line was found
                    header_skipped = True
                    logger.debug(
                        f"Index header/separator identified in {source_id} around line {line_num + 1}"
                    )
                # else: still in descriptive part or haven't found header

                continue  # Skip this line (header part or separator or descriptive line)

            # --- Line Processing after Header ---
            if not line:  # Skip empty lines encountered after header/separator
                continue

            parts = line.split('|')
            if len(parts) != 5:
                # Log only once per source if format seems consistently wrong
                if errors_in_source < 5:  # Limit logging spam for bad files
                    logger.warning(
                        f"Skipping malformed line #{line_num + 1} in {source_id} "
                        f"(Expected 5 parts, got {len(parts)}): {line[:150]}..."
                    )
                elif errors_in_source == 5:
                    logger.warning(
                        f"Further malformed line errors suppressed for {source_id}."
                    )
                errors_in_source += 1
                continue

            try:
                # Extract data based on column order
                cik = parts[0].strip()
                # company_name = parts[1].strip() # Not storing this field
                form_type = parts[2].strip().upper()  # Normalize form type
                date_filed_str = parts[3].strip()
                filename_path = parts[4].strip()

                # --- Basic Validation before filtering ---
                if not cik.isdigit():
                    if errors_in_source < 5:
                        logger.warning(
                            f"Skipping line #{line_num + 1} in {source_id} due to non-numeric CIK '{cik}'"
                        )
                    errors_in_source += 1
                    continue

                # --- FILTERING by Form Type (if target_forms is provided) ---
                if target_forms is not None and form_type not in target_forms:
                    continue  # Skip if not a form type we are interested in

                # --- DATA EXTRACTION & CLEANING (if form passes filter or no filter) ---
                # Parse date
                try:
                    filing_date = datetime.strptime(date_filed_str,
                                                    '%Y-%m-%d').date()
                except ValueError:
                    if errors_in_source < 5:
                        logger.warning(
                            f"Skipping line #{line_num + 1} in {source_id} due to invalid date '{date_filed_str}'"
                        )
                    errors_in_source += 1
                    continue

                # Extract Accession Number using helper
                accession_number = self._extract_accession_number(
                    filename_path, line_num + 1, source_id)
                if not accession_number:
                    # Warning already logged by helper
                    errors_in_source += 1
                    continue

                # Extract Primary Document Filename (heuristic: part after last '/')
                # Store what the index *says*, finding the *real* HTM is later
                primary_doc_name = filename_path.split('/')[-1]

                filings_found.append({
                    "cik":
                    cik,
                    "form_type":
                    form_type,
                    "filing_date":
                    filing_date,
                    "accession_number":
                    accession_number,
                    "primary_document_filename":
                    primary_doc_name
                })

            except Exception as e:
                # Catch unexpected errors processing a single line
                # Log error but continue processing other lines
                if errors_in_source < 5:
                    logger.error(
                        f"Error processing line #{line_num + 1} in {source_id}: {e} - Line: {line[:150]}...",
                        exc_info=False)  # Keep log cleaner for per-line errors
                elif errors_in_source == 5:
                    logger.warning(
                        f"Further line processing errors suppressed for {source_id}."
                    )
                errors_in_source += 1
                continue  # Continue to next line

        # Check if we ever actually finished skipping the header
        if not header_skipped:
            logger.error(
                f"Could not find expected header separator ('---' after header line) in index source: {source_id}"
            )
            # This likely means the input was empty, malformed, or not an index file.
            raise IndexParsingError(
                "Failed to find header separator in index file",
                source=source_id)

        logger.info(
            f"Finished parsing index source '{source_id}'. Processed {line_count} lines, "
            f"found {len(filings_found)} relevant filings, encountered {errors_in_source} line errors."
        )

        return filings_found

    def _extract_accession_number(self, filename_path: str, line_num: int,
                                  source_desc: Optional[str]) -> Optional[str]:
        """Helper to extract accession number reliably from filename path."""
        # Standard format: edgar/data/CIK/ACCESSION_NODASH/ACCESSION-WITH-DASHES.txt (or .htm)
        # Or sometimes just: edgar/data/CIK/ACCESSION_NODASH.txt

        # Preferentially match the version WITH dashes if present
        accession_match = re.search(r'(\d{10}-\d{2}-\d{6})', filename_path)
        if accession_match:
            return accession_match.group(1).strip()
        else:
            # Fallback 1: Try finding 18 digits (no dashes version)
            accession_no_dash_match = re.search(
                r'(\d{18})', filename_path)  # Look for exactly 18 digits
            if accession_no_dash_match:
                acc_no_dash = accession_no_dash_match.group(1)
                accession_number = f"{acc_no_dash[:10]}-{acc_no_dash[10:12]}-{acc_no_dash[12:]}"
                logger.debug(
                    f"Constructed accession number {accession_number} from 18-digit version on line {line_num} in {source_desc}"
                )
                return accession_number.strip()
            else:
                # Fallback 2: Look for 20 digits (sometimes includes sequence?)
                accession_20_digit_match = re.search(r'(\d{20})',
                                                     filename_path)
                if accession_20_digit_match:
                    acc_20_digit = accession_20_digit_match.group(1)
                    accession_number = f"{acc_20_digit[:10]}-{acc_20_digit[10:12]}-{acc_20_digit[12:18]}"  # Take first 6 of last part
                    logger.debug(
                        f"Extracted 18-digit acc num {accession_number} from 20 digits on line {line_num} in {source_desc}"
                    )
                    return accession_number.strip()
                else:
                    # If no patterns match, log warning and return None
                    logger.warning(
                        f"Could not extract accession number from path on line #{line_num} in {source_desc}: {filename_path}"
                    )
                    return None
